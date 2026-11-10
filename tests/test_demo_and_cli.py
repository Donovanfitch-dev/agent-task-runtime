from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from agent_task_runtime.cli import main
from agent_task_runtime.demo import run_demo


def capture_demo() -> list[str]:
    lines: list[str] = []
    assert run_demo(out=lines.append) == 0
    return lines


def test_demo_tells_the_whole_story() -> None:
    text = "\n".join(capture_demo())
    for marker in (
        "created=True",
        "created=False",
        "worker-A  claimed task-demo-1: attempt=1, fencing token=1",
        "CRASH before step 'analyze'",
        "checkpoints on disk: ['plan', 'gather']",
        "lease expired -> task-demo-1 back to 'queued'",
        "worker-B  claimed task-demo-1: attempt=2, fencing token=2",
        "checkpoints found: ['plan', 'gather']",
        "result.ran=['analyze', 'draft', 'review']",
        "complete(token=1) REJECTED",
        "unchanged: state=succeeded",
        "closing the database and reopening the file",
        "FAILED (SinkUnavailableError",
        "duplicate delivery ignored",
        "effects applied=1",
        "invariants held",
    ):
        assert marker in text, marker
    assert text.count("FAILED (SinkUnavailableError") == 2


def test_demo_output_is_deterministic_and_leaks_no_paths() -> None:
    first, second = capture_demo(), capture_demo()
    assert first == second
    assert "agent-task-runtime-demo-" not in "\n".join(first)


def test_demo_runs_as_a_module() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "agent_task_runtime", "demo"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "invariants held" in proc.stdout


def test_cli_enqueue_stats_work_inspect_dispatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = str(tmp_path / "cli.db")
    args = ["enqueue", "--db", db, "--kind", "agent.research", "--key", "k1", "--priority", "3"]
    assert main([*args, "--payload", '{"topic": "example"}']) == 0
    assert "created" in capsys.readouterr().out
    assert main([*args, "--payload", '{"topic": "example"}']) == 0
    assert "already exists" in capsys.readouterr().out

    assert main(["stats", "--db", db]) == 0
    assert "queued         1" in capsys.readouterr().out

    assert main(["work", "--db", db, "--once"]) == 0
    assert "succeeded" in capsys.readouterr().out

    assert main(["inspect", "--db", db]) == 0
    listing = capsys.readouterr().out
    assert "succeeded" in listing
    task_id = listing.split()[0]

    assert main(["inspect", "--db", db, task_id]) == 0
    detail = capsys.readouterr().out
    assert '"state": "succeeded"' in detail and "claimed" in detail and "task.succeeded" in detail

    assert main(["dispatch", "--db", db, "--once"]) == 0
    line = json.loads(capsys.readouterr().out.strip())
    assert line["event_type"] == "task.succeeded" and line["key"] == f"{task_id}:task.succeeded"

    assert main(["stats", "--db", db]) == 0
    assert "delivered      1" in capsys.readouterr().out


def test_cli_rejects_bad_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / "bad.db")
    assert main(["enqueue", "--db", db, "--kind", "k", "--payload", "{nope"]) == 2
    assert main(["enqueue", "--db", db, "--kind", "k", "--payload", "[1]"]) == 2
    assert "error" in capsys.readouterr().err


def test_cli_reports_runtime_errors_without_a_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = str(tmp_path / "e.db")
    assert main(["inspect", "--db", db, "missing-task"]) == 1
    assert "no such task" in capsys.readouterr().err
