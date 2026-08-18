"""Tests for the PlotJuggler session export (flattened CSV + framed layout)."""

import pathlib
import xml.etree.ElementTree as ET

import duckdb
import pytest

import server
from settings import settings
from src.pipeline import plotjuggler

TS = settings.TIMESTAMP_SECONDS_COLUMN_NAME
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
    return server.export_for_plotjuggler(**params)


def test_layout_matches_plotjuggler_schema() -> None:
    result = _export()
    root = ET.parse(result["layout"]).getroot()  # noqa: S314 -- parsing our own output

    plot = root.find("./tabbed_widget/Tab/Container/DockSplitter/DockArea/plot")
    assert plot is not None
    assert plot.get("mode") == "TimeSeries"

    plot_range = plot.find("range")
    assert float(plot_range.get("left")) == 15.0
    assert float(plot_range.get("right")) == 25.0
    assert float(plot_range.get("bottom")) < float(plot_range.get("top"))

    # Absolute time so the framed range matches the data's epoch seconds.
    assert root.find("use_relative_time_offset").get("enabled") == "0"

    # The CSV loader defaults: our timestamp column is preconfigured as the time axis.
    csv_plugin = root.find("./Plugins/plugin[@ID='DataLoad CSV']/default")
    assert csv_plugin.get("time_axis") == TS

    # The layout references the data file, so `plotjuggler -l layout` reloads it.
    file_info = root.find("./previouslyLoaded_Datafiles/fileInfo")
    assert file_info.get("filename") == result["csv"]
    assert file_info.find("plugin").get("ID") == "DataLoad CSV"


def test_every_curve_is_a_csv_column() -> None:
    result = _export()
    header = pathlib.Path(result["csv"]).read_text().splitlines()[0].split(",")
    root = ET.parse(result["layout"]).getroot()  # noqa: S314 -- parsing our own output
    curve_names = [curve.get("name") for curve in root.iter("curve")]

    assert curve_names == result["curves"]
    assert curve_names, "at least one curve must be plotted"
    for name in curve_names:
        assert name in header, f"curve '{name}' must match a CSV column exactly"


def test_csv_is_windowed_and_command_points_at_layout() -> None:
    result = _export()
    lines = pathlib.Path(result["csv"]).read_text().splitlines()
    timestamps = [float(line.split(",")[0]) for line in lines[1:]]
    assert timestamps, "window must contain data"
    assert min(timestamps) >= 15.0
    assert max(timestamps) <= 25.0
    assert result["layout"] in result["command"]
    assert "brake_event_1" in result["layout"]  # name is snake_cased


def test_signals_filter_and_unknown_signal_error() -> None:
    result = _export(signals=["message/accel_x"])
    assert result["curves"] == ["message/accel_x"]

    with pytest.raises(ValueError, match="Unknown or non-numeric signals"):
        _export(signals=["message/not_a_signal"])


def test_non_numeric_signal_rejected_cleanly() -> None:
    relation = duckdb.sql(
        f"SELECT 1.0 AS \"{TS}\", struct_pack(temp := 20.0, mode := 'auto') AS sensor"
    )
    # "sensor/mode" exists after flattening but is a string -- it must fail with a
    # clean ValueError, not a TypeError from the y-range computation.
    with pytest.raises(ValueError, match="non-numeric"):
        plotjuggler.export_window(
            relation,
            name="x",
            start_seconds=0.0,
            end_seconds=2.0,
            signals=["sensor/mode"],
        )


def test_curve_cap_applies_without_explicit_signals() -> None:
    columns = ", ".join(f"s{i:02d} := {float(i)}" for i in range(12))
    relation = duckdb.sql(f'SELECT 1.0 AS "{TS}", struct_pack({columns}) AS wide')
    result = plotjuggler.export_window(relation, name="wide", start_seconds=0.0, end_seconds=2.0)
    assert len(result["curves"]) == plotjuggler.MAX_CURVES
