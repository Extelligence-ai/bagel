"""Flows over external matcha-ext recordings with known anomalies."""

import pathlib

import pytest

import server
from src.logging.base import NoLoggingTopicsFoundError
from test._fixtures import external


def _recordings() -> list[pathlib.Path]:
    return external.griddle_recordings()


@pytest.mark.skipif(not _recordings(), reason="external fixtures unavailable")
@pytest.mark.parametrize("recording", _recordings(), ids=lambda p: p.stem)
def test_recording_is_describable(recording: pathlib.Path) -> None:
    result = server.describe_data_source(str(recording))
    assert isinstance(result, list) and result


@pytest.mark.skipif(not _recordings(), reason="external fixtures unavailable")
def test_gps_loss_recovery_surfaces_logs() -> None:
    root = external.require_external()
    rec = root / "px4-gps-loss-recovery.mcap"
    if not rec.exists():
        pytest.skip("gps-loss recording missing")
    # KNOWN FINDING: this px4-sourced recording has no rcl_interfaces/msg/Log
    # topics, and read_loggings raises NoLoggingTopicsFoundError instead of
    # returning an empty list as its own docstring implies ("Not all sources
    # provide logs"). Characterized here rather than hidden; see
    # docs/superpowers/reports/2026-07-26-base-scorecard.md, "Task 7 — E2E
    # findings".
    with pytest.raises(NoLoggingTopicsFoundError):
        server.read_loggings(str(rec))
