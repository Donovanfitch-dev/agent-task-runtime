# Design notes

This is an independent reference implementation, written from scratch for demonstration;
not extracted from any production system. This document explains *why* the code is shaped
the way it is. For *what* it guarantees, see the README.

## 1. Goals and non-goals

**Goals**

* Make the failure modes of long-running agent jobs (crash, stall, duplicate, zombie) explicit
  and individually testable.
* Keep the protocol small enough to read in one sitting: leases, fencing tokens,
  checkpoints, a transactional outbox, an audit log.
* Be runnable and testable with no infrastructure and no third-party runtime dependencies.
* Be deterministic under test: clock and RNG are injected, so no test sleeps.

**Non-goals**

* Throughput, horizontal scale, multi-host operation, multi-tenancy, authentication.
* Any real model or network call. Executors here are simulations.

## 2. Data model

| Table | Key columns | Notes |
|---|---|---|
| `tasks` | `seq` (autoincrement), `id`, `state`, `priority`, `attempt`, `releases`, `max_attempts`, `not_before`, `lease_owner`, `lease_expires_at`, `lease_seconds`, `idempotency_key` (unique) | `seq` gives FIFO order; `attempt` is the fencing token (it counts every claim); `attempt - releases` is the retry budget used; `lease_seconds` is the claimed duration that heartbeats renew with |
| `task_events` | `task_id`, `at`, `actor`, `event_type`, `from_state`, `to_state`, `attempt`, `detail` | Append-only, enforced by triggers |
| `checkpoints` | `(task_id, step)` primary key, `attempt`, `data` | One row per step, upserted |
| `outbox` | `idempotency_key` (unique), `status`, `attempts`, `next_attempt_at` | Written in the same transaction as the terminal transition |
| `schema_migrations` | `version`, `name`, `applied_at` | Run-once bookkeeping |

Timestamps are `REAL` seconds from the injected clock. Payloads, results and event details
are canonical JSON text (sorted keys, compact separators), which also makes the
idempotency-conflict comparison order-insensitive.

## 3. Concurrency model

SQLite permits many readers but one writer. Every mutating `TaskStore` method opens its
transaction with `BEGIN IMMEDIATE`, which acquires the write lock *before* any read. That is
the whole claim protocol:

```text
BEGIN IMMEDIATE
  SELECT the best queued, eligible task        -- priority DESC, seq ASC
  UPDATE it: leased, attempt = attempt + 1, owner, deadline
  INSERT audit row
COMMIT
```

Because the lock is taken up front, the `SELECT` and the `UPDATE` cannot interleave with
another claimer's. There is no compare-and-swap loop, no retry-on-conflict, and no window in
which two workers read the same "next" task. A plain `BEGIN` (deferred) would be wrong: two
claimers could both read the same row and then one would fail on upgrade.

Each worker owns its own connection (`sqlite3` connections are bound to a thread by
default, and the store keeps that check on). Settings: `journal_mode=WAL` so readers do not
block the writer, `synchronous=FULL` so a committed transaction is durable, `foreign_keys=ON`,
and a long busy timeout so contending writers queue rather than error.

One detail found by running the concurrency tests repeatedly: the *first* switch of a fresh
file into WAL mode needs an exclusive lock, and SQLite may report "database is locked"
immediately instead of honouring the busy timeout. The store retries that single `PRAGMA`
briefly; once WAL is set, it is persistent and later opens are no-ops.

## 4. Leases and fencing tokens

A **lease** answers "who may work on this task right now?" with a deadline. A lease alone
is not enough, because a process cannot reliably know whether its own lease has lapsed: it
may be paused (GC, a stopped container, a slow syscall) between checking and acting.

A **fencing token** closes that gap. Each claim increments `tasks.attempt`, and the new value
is the token handed to the worker. Every subsequent write presents its token, and the store
applies it only if the task is still `leased` *and* the token equals the current `attempt`.
The check and the write happen in the same `BEGIN IMMEDIATE` transaction, so no claim can
slip between them.

```text
A claims          token 1
A stalls          lease expires
reaper            requeues
B claims          token 2
A wakes, writes   token 1 != 2   -> StaleLeaseError, nothing written
B writes          token 2 == 2   -> accepted
```

Using the claim counter as the token means there is no separate sequence to keep in sync,
and monotonicity is guaranteed by the same transaction that grants the lease.

What fencing does **not** do: it cannot stop a zombie from performing side effects in other
systems before it next touches the store. To protect an external system, pass
`ctx.fencing_token` to it and have it reject lower tokens than it has already seen.

### Semantics worth stating precisely

* **Complete and fail ignore the deadline.** If the token is current and the task is still
  `leased`, nobody else holds it, so finished work is accepted even if the deadline passed
  and the reaper has not yet run. Rejecting it would throw away completed work for no
  safety benefit.
* **Heartbeat does not.** A heartbeat on an already-expired lease raises `LeaseExpiredError`
  (a subclass of `StaleLeaseError`): the worker has been told, in effect, to stop, and
  silently resurrecting a lapsed lease would make deadlines meaningless. The two behaviours
  are deliberately different and both are tested.
* **Checkpoints follow the `complete` rule, not the heartbeat rule.** A checkpoint records
  finished work and does not extend ownership, so it is accepted while the token is current
  even past the deadline (and tested). Once the task is reclaimed the token is stale and the
  checkpoint is rejected. The rule of thumb: operations that record finished work are
  accepted for a current token; operations that extend ownership are not.
* **Heartbeats renew with the claim's duration.** The duration is stored on the task at
  claim time (`lease_seconds`) and `TaskContext` keeps it too, so a task claimed with a long
  lease keeps it. Only an explicit `lease_seconds` argument changes it.
* **Graceful release advances the token but not the budget.** `attempt` must keep rising
  (it is the token), so a release increments `releases`; `max_attempts` bounds
  `attempt - releases`, i.e. failed or lost attempts. The alternative, charging releases to
  the budget, would let a rolling restart dead-letter healthy work. The cost is that a task
  released forever is never stopped by the cap.
* **Rejected operations write nothing, not even an audit row.** The tests snapshot the task,
  its events, its checkpoints and its outbox, attempt the stale call, and compare.
* **Cancelling a leased task** moves it to `cancelled`; the worker's token is then stale
  because the state is no longer `leased`.

## 5. The reaper

`reap_expired()` finds `leased` tasks whose deadline is `<=` now. For each it either
re-queues the task, or, if the retry budget (`attempt - releases`) is used up, dead-letters it (and writes the outbox
event). It is idempotent, so any number of workers can run it, and `Worker.run_once` calls it
before each claim by default.

Policy choice: a reaped task becomes eligible **immediately**, with no backoff. Backoff
exists to give a failing dependency time to recover; a vanished worker says nothing about
the task. Reaping still counts the attempt, so a task that keeps killing its workers
eventually dead-letters instead of looping forever.

## 6. Retry policy

```text
raw   = min(max_delay, base_delay * multiplier ** (attempt - 1))
delay = raw * (1 - jitter * u),   u in [0, 1) from the injected RNG
```

The exponentiation saturates at `max_delay` for any attempt number (an `OverflowError` from the
float power is caught and treated as the cap), so an event with a huge attempt count cannot
break delivery. The backoff exponent for task retries is the budget used, not the claim count.

Jitter only *shortens* the delay, so `max_delay` stays a true bound and the schedule for a
given RNG stream is reproducible. The default `RetryPolicy` is 5 attempts, 1 s base, factor 2,
60 s cap, jitter 0.5; these are illustrative defaults, not tuned values.

The backoff part is a separate class, `Backoff`, with no attempt limit; `RetryPolicy` adds
`max_attempts` on top, and outbox delivery uses a bare `Backoff` because delivery has no limit.

Failure classification is the executor's job: `PermanentError` fails the task without retry,
`ShutdownRequested` releases it, and every other exception is retried. Unknown exceptions
default to *retryable* because a false "permanent" verdict loses work, while a false
"retryable" one only costs bounded attempts.

## 7. Checkpoints

Executors call `ctx.checkpoint(step, data)` after each step. Checkpoints are fenced writes,
so a zombie cannot overwrite the new owner's progress. On a later attempt, `ctx.checkpoints`
already contains everything saved so far and the executor skips those steps. The stored
`attempt` records which attempt wrote each step.

Checkpoints give *step-level* resumption, not exactly-once steps: a step that completed its
side effect but crashed before checkpointing will run again. Keep steps idempotent or make the
checkpoint part of the same transaction as the step's effect where you can.

## 8. Transactional outbox

The dual-write problem: "mark the task succeeded" and "tell another system" are two writes
to two systems, and any ordering of them can lose or invent an event on a crash. The outbox
turns it into one local write plus one retried delivery.

```text
BEGIN IMMEDIATE
  UPDATE tasks SET state = 'succeeded' ...
  INSERT INTO task_events ...
  INSERT INTO outbox ...                 -- same transaction
COMMIT
-- later, independently, possibly after a restart:
dispatcher: sink.deliver(event); on success UPDATE outbox SET status = 'delivered'
```

Failure windows, and what happens in each:

| Crash or failure point | Outcome |
|---|---|
| Before commit | Nothing happened: task still `leased`, no event, lease eventually expires |
| After commit, before dispatch | Event is in the file; the next dispatcher run sends it |
| Sink fails | Failure and next-attempt time recorded; event is retried with backoff |
| Sink applies, acknowledgement lost | Event is sent again with the **same** idempotency key |
| Sink acks, crash before `delivered` is recorded | Same: sent again, same key |

The last two rows are why delivery is **at-least-once**: it is impossible for the dispatcher to
distinguish "applied, ack lost" from "never applied". The cure is on the consumer side. Every
event has a stable key (`<task_id>:<event_type>`), and `DedupSink` demonstrates an idempotent
receiver that records processed keys in its own database. It has two modes. In
`transactional=True` mode the handler receives the sink's connection, so an effect written to
that database commits or rolls back with the key: a crash between effect and commit leaves
neither, and redelivery applies the effect once (tested). In the default mode the handler
runs inside the transaction but its effect is outside it; a crash after an external effect
repeats the effect on redelivery (also tested). Only the first mode is effectively-once, and
only for effects stored in that same database.

The dispatcher isolates events from each other: a sink failure or a bookkeeping error for
one event is recorded (`DispatchReport.failed` / `.errors`) and the batch continues. It does
not coordinate with other dispatchers: outbox rows are not claimed, so two dispatchers on
one database can send the same row. That is allowed by at-least-once but is not designed for.

Terminal outbox events: `task.succeeded`, `task.failed`, `task.dead_lettered`,
`task.cancelled`. Retries and re-queues are not terminal and emit none. Because a task
reaches at most one terminal state, the key is unique per task and event type.

## 9. Audit log

`task_events` records actor, time, event type, from-state, to-state and attempt for every
transition, inside the transition's own transaction, so log and state cannot disagree.
`BEFORE UPDATE` and `BEFORE DELETE` triggers abort with `task_events is append-only`.
Heartbeats and checkpoints are intentionally not logged: they are high-frequency and not
state transitions. The triggers stop accidental or application-level mutation; they are not a
defence against someone with write access to the file.

## 10. Migrations

Migrations are `(version, name, sql)` tuples applied in order. Each one runs in its own
`BEGIN IMMEDIATE` transaction together with its row in `schema_migrations`, so a failure
rolls back to the previous version with no half-applied schema. The applied set is re-read
*inside* the transaction, so two processes starting against a fresh file cannot both apply a
version. If the database contains a version this code does not know, startup is refused: an
older binary must not run against a newer schema. SQL is split into statements with
`sqlite3.complete_statement`, which keeps `CREATE TRIGGER ... BEGIN ... END` bodies intact.

## 11. Time

All time comes from an injected `Clock`. `SystemClock` is `time.time()`; `ManualClock` only
moves when told to. This makes every timing behaviour (lease expiry, heartbeat renewal,
backoff windows) testable by calling `advance()`. The cost of the production choice is real:
`time.time()` is not monotonic and can step, so a clock adjustment can expire leases early
or late. All workers must therefore share a host clock. A multi-node design should use the
database server's clock for lease deadlines.

## 12. Worker and executor contract

```text
claim -> build TaskContext (task, checkpoints) -> executor.execute(ctx)
      -> complete(result)                  on return (unserialisable result: fail permanently)
      -> fail(retryable=False)             on PermanentError
      -> release                           on ShutdownRequested
      -> fail(retryable=True)              on any other Exception
      -> outcome "lease_lost", no write    on StaleLeaseError
```

`SimulatedCrash` derives from `BaseException` on purpose. The worker catches `Exception`, so
a simulated crash propagates out leaving the task `leased`, exactly as a killed process would.
The demo and the checkpoint tests rely on this to model a hard crash without spawning
processes.

Errors from recording the result are the runtime's, not the executor's. A result that cannot
be serialised (`TypeError`/`ValueError` from `complete`) fails the task permanently with
`ResultNotRecordable`, because a retry would re-run the executor's side effects only to fail
the same way. Any other store error propagates and leaves the task leased for the reaper.

Graceful shutdown is cooperative: `Worker.stop()` sets an event; the loop finishes (or the
executor releases, via `ctx.raise_if_stopping()`) the current task and does not claim another.
Idle polling uses `Event.wait`, so `stop()` interrupts it immediately.

## 13. Testing approach

* **Table-driven state machine test** over every `(from, to)` pair.
* **Concurrency tests** use threads with separate connections released by a barrier:
  one task and sixteen claimers (exactly one winner), sixty tasks and eight claimers (each
  claimed exactly once), and concurrent enqueues with one idempotency key.
* **Fencing tests** run the full zombie scenario and parametrise over every fenced operation,
  asserting the exception *and* that the complete observable state is unchanged.
* **Atomicity test** injects a failure into the outbox insert and asserts the terminal state
  and audit row were rolled back too.
* **Restart tests** close the store and reopen the same database file: checkpoints, pending
  outbox rows and their backoff deadlines all survive.
* **No sleeping.** Time moves only via `ManualClock.advance`. The one place real time appears,
  the CLI's long-running loops, is outside the automated suite.

## 14. What would change for Postgres or multiple nodes

The protocol carries over unchanged; the mechanics differ.

| Concern | SQLite (here) | Postgres or multi-node |
|---|---|---|
| Atomic claim | `BEGIN IMMEDIATE` | `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` in a transaction |
| Fencing token | `attempt` column | Same column; use `bigint` |
| Lease clock | Worker wall clock | Database `now()`; removes the shared-clock assumption |
| Heartbeat | Explicit call between steps | Background thread so a blocked step cannot lose its lease |
| Outbox dispatch | One dispatcher assumed | Several dispatchers need `SKIP LOCKED` on outbox rows or advisory locks; ordering needs a partition key |
| Outbox transport | Polling | Polling, or logical decoding / `LISTEN`/`NOTIFY` as a wake-up hint |
| Dead outbox rows | Retried forever | Add a poison state and an operator queue |
| Migrations | Hand-rolled, in-process | A real migration tool, run once at deploy time |

None of this is implemented; the table is a map, not a promise.

## 15. Known gaps

* No background heartbeat, so a single blocking call longer than the lease will lose it.
* A task that is released repeatedly is never stopped by `max_attempts` (see section 4).
* Crash-safety rests on SQLite and is tested with threads and close/reopen, not by killing
  processes or cutting power.
* No retention policy; `task_events`, `checkpoints` and delivered outbox rows grow forever.
* No per-kind concurrency limits, rate limiting, dependencies between tasks, or scheduled
  (cron-like) enqueue. `not_before` provides delayed start only.
* Result and payload size are unbounded.
