"""Tests for batch mode: running one pipeline across many data sources."""

import pathlib

import pytest

from bagel import server
from bagel.pipeline import batch
from bagel.settings import settings

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"


def _config() -> dict:
    return {
        "name": "csv_batch",
        "site": "test_site",
        "asset": "test_asset",
        "path": "PLACEHOLDER",  # overridden per source
        "allow_failure": False,
        "cadence": {"topic": "message", "when": "once_at_end"},
        "tasks": [
            {
                "module": "bagel.pipeline.tasks.write_topics_to_file",
                "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
                "args": {"topics": ["message"], "output_format": "csv"},
            }
        ],
    }


def _write_csv(path: pathlib.Path) -> None:
    path.write_text("t,accel_x\n0.0,-0.5\n1.0,-12.0\n2.0,-1.0\n")


def test_expand_paths_globs_and_dedupes(tmp_path: pathlib.Path) -> None:
    (tmp_path / "a.csv").write_text("x")
    (tmp_path / "b.csv").write_text("x")
    pattern = str(tmp_path / "*.csv")
    literal = str(tmp_path / "a.csv")
    expanded = batch.expand_paths([pattern, literal])  # literal duplicates a.csv
    assert expanded == sorted([str(tmp_path / "a.csv"), str(tmp_path / "b.csv")])


def test_expand_paths_keeps_unmatched_pattern_literal() -> None:
    assert batch.expand_paths(["./does/not/exist"]) == ["./does/not/exist"]


def test_run_batch_processes_each_source(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    sources = []
    for name in ("log_a.csv", "log_b.csv"):
        source = tmp_path / name
        _write_csv(source)
        sources.append(str(source))

    results = batch.run_batch(_config(), sources)
    assert [r["status"] for r in results] == ["completed", "completed"]
    assert all(len(r["artifacts"]) == 1 for r in results)
    # Distinct sources -> distinct artifact paths (log_id derives from the path).
    assert results[0]["artifacts"][0] != results[1]["artifacts"][0]


def test_run_batch_continues_past_a_failing_source(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    good = tmp_path / "good.csv"
    _write_csv(good)
    missing = str(tmp_path / "missing.csv")  # does not exist -> resolve fails

    results = batch.run_batch(_config(), [str(good), missing])
    statuses = {r["path"]: r["status"] for r in results}
    assert statuses[str(good)] == "completed"
    assert statuses[missing] == "failed"
    assert "error" in next(r for r in results if r["status"] == "failed")


def test_summarize_counts(tmp_path: pathlib.Path) -> None:
    results = [
        {"path": "a", "status": "completed", "artifacts": ["x", "y"]},
        {"path": "b", "status": "completed", "artifacts": ["z"]},
        {"path": "c", "status": "failed", "error": "boom"},
    ]
    summary = batch.summarize(results)
    assert summary["sources"] == 3
    assert summary["completed"] == 2
    assert summary["failed"] == 1
    assert summary["artifacts"] == 3


def test_run_pipeline_batch_tool_end_to_end(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    for name in ("one.csv", "two.csv"):
        _write_csv(tmp_path / name)
    summary = server.run_pipeline_batch(_config(), [str(tmp_path / "*.csv")])
    assert summary["sources"] == 2
    assert summary["completed"] == 2
    assert summary["failed"] == 0
    assert summary["artifacts"] == 2
