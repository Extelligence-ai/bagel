"""Tests for the Rerun recording export (.rrd), verified by reading the file back."""

import pathlib

import pytest

import server
from settings import settings

rr = pytest.importorskip("rerun", reason="rerun-sdk is optional (uv sync --group viz)")

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"
SAMPLE_ARGS = {"timestamp_column": "t", "timestamp_format": "seconds"}


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def _export(**overrides: object) -> dict:
    params = {
        "path": SAMPLE,
        "topics": ["message"],
        "start_seconds": 15.0,
        "end_seconds": 25.0,
        "name": "brake event 1",
        "args": SAMPLE_ARGS,
    }
    params.update(overrides)
    return server.export_for_rerun(**params)


def test_writes_readable_recording_with_window_contents() -> None:
    import rerun.dataframe as rrd

    result = _export()

    rrd_file = pathlib.Path(result["rrd"])
    assert rrd_file.exists()
    assert rrd_file.name == "brake_event_1.rrd"
    assert result["command"] == f"rerun '{rrd_file}'"

    # Read the recording back: every exported signal is present with the window's rows.
    recording = rrd.load_recording(str(result["rrd"]))
    for signal in result["signals"]:
        view = recording.view(index="time", contents=signal)
        table = view.select().read_all()
        assert table.num_rows == 21  # 15.0..25.0 inclusive at 0.5s cadence
        times = [t.total_seconds() for t in table.column("time").to_pylist()]
        assert min(times) == 15.0
        assert max(times) == 25.0


def test_signal_selection_and_validation() -> None:
    picked = _export(signals=["message/vel"])
    assert picked["signals"] == ["message/vel"]

    with pytest.raises(ValueError, match="Unknown or non-numeric"):
        _export(signals=["message/no_such_field"])


def test_all_numeric_signals_by_default() -> None:
    result = _export()
    assert set(result["signals"]) == {"message/t", "message/accel_x", "message/vel"}
