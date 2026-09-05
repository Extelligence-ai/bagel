"""Cadence validation, consistent source time, and truthful outcomes."""

from collections.abc import Iterator
from unittest.mock import Mock

import pytest

import server
from src.pipeline import base, windows


def _config() -> dict:
    return {
        "name": "review",
        "site": "test",
        "asset": "robot",
        "path": "data/sample/pyarrow/csv/flight.csv",
        "allow_failure": False,
        "source_args": {"timestamp_column": "t", "timestamp_format": "seconds"},
        "cadence": {"topic": "message", "when": "once_at_end"},
        "tasks": [],
    }


@pytest.mark.parametrize("every", [0, -1, True])
def test_invalid_frequency_is_rejected_before_source_access(every: int) -> None:
    config = _config()
    config["path"] = "/missing.csv"
    config["cadence"]["when"] = {"every": every, "unit": "frame"}
    with pytest.raises(ValueError):
        base.Pipeline.build(config)


def test_negative_lookback_and_frame_event_delay_are_rejected() -> None:
    with pytest.raises(ValueError):
        base.Lookback(last=-1, unit=base.Unit.SECOND)
    with pytest.raises(ValueError, match="time unit"):
        base.OnEvent(predicate="TRUE", forward=base.Lookback(last=1, unit=base.Unit.FRAME))


def test_cadence_uses_shared_source_timestamp_options() -> None:
    pipeline = base.Pipeline.build(_config())
    assert list(pipeline._asof_timestamps()) == [59.5]


def test_legacy_setup_is_shared_with_cadence() -> None:
    config = _config()
    config["tasks"] = [
        {
            "module": "src.pipeline.tasks.write_topics_to_file",
            "setup": config.pop("source_args"),
            "args": {"topics": ["message"], "output_format": "csv"},
        }
    ]
    pipeline = base.Pipeline.build(config)
    assert list(pipeline._asof_timestamps()) == [59.5]
    assert "setup" in config["tasks"][0]


def test_conflicting_source_options_fail_before_operator_import() -> None:
    config = _config()
    config["tasks"] = [{"module": "missing.module", "setup": {"timestamp_column": "other"}}]
    with pytest.raises(ValueError, match="Conflicting source option"):
        base.Pipeline.build(config)


def test_permitted_failures_do_not_report_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config()
    config["allow_failure"] = True
    pipeline = base.Pipeline.build(config)
    task = Mock(upload=False)
    task.execute.side_effect = ValueError("broken task")
    pipeline._tasks = [(task, None)]
    monkeypatch.setattr(base.Pipeline, "build", lambda config: pipeline)
    result = server.run_pipeline(config)
    assert result["status"] == "failed"
    assert result["runs"]["failed"] == 1
    assert "broken task" in result["runs"]["errors"][0]
    task.execute.side_effect = None
    task.execute.return_value = None
    pipeline.run_at(61.0)
    assert pipeline.summary.status == "partial"


def test_gate_skip_is_counted() -> None:
    pipeline = base.Pipeline.build(_config())
    gate = Mock()
    gate.evaluate.return_value = False
    pipeline._gates = [(gate, None)]
    pipeline.run_at(60.0)
    assert pipeline.summary.skipped == 1
    assert pipeline.summary.failed == pipeline.summary.succeeded == 0


def test_retention_is_clipped_to_source_bounds() -> None:
    plan = windows.plan_reduction([(0.0, True), (10.0, False)], 20, 20, 10, bounds=(0, 10))
    assert plan["intervals"] == [(0, 10)]
    assert plan["kept_seconds"] == 10
    assert plan["kept_fraction"] == 1


def test_sorted_event_stream_is_consumed_incrementally() -> None:
    consumed = []

    def samples() -> Iterator[tuple[float, bool]]:
        for index in range(100_000):
            consumed.append(index)
            yield float(index), index % 2 == 0

    events = windows.iter_rising_edges(samples())
    assert next(events) == 0
    assert consumed == [0]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1])
def test_invalid_window_values_are_rejected(value: float) -> None:
    with pytest.raises(ValueError):
        windows.plan_reduction([(0.0, True)], value, 0, 1)
