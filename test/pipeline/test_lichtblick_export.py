"""Tests for the Lichtblick session export (JSON-channel MCAP + framed layout).

The strongest check dogfoods Bagel itself: the exported MCAP is read back through
Bagel's own generic MCAP source (the jsonschema reader), proving the file is valid
MCAP with usable schemas -- if Bagel can query it, Lichtblick can plot it.
"""

import json
import pathlib

import pytest

import server
from settings import settings

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
    return server.export_for_lichtblick(**params)


def test_layout_matches_lichtblick_schema() -> None:
    result = _export()
    layout = json.loads(pathlib.Path(result["layout"]).read_text())

    assert layout["layout"] == "Plot!bagel"
    assert set(layout) == {"configById", "globalVariables", "userNodes", "playbackConfig", "layout"}

    plot = layout["configById"]["Plot!bagel"]
    assert plot["xAxisVal"] == "timestamp"
    assert plot["minXValue"] == 15.0
    assert plot["maxXValue"] == 25.0
    assert plot["minYValue"] < plot["maxYValue"]
    assert {"value", "enabled", "timestampMethod"} <= set(plot["paths"][0])
    assert "message.vel" in {p["value"] for p in plot["paths"]}


def test_exported_mcap_is_readable_by_bagel_itself() -> None:
    result = _export()

    # Dogfood: resolve + query the export through Bagel's own MCAP jsonschema reader.
    from src.di.types.data_source import DataSource, resolve

    assert resolve(result["mcap"]) is DataSource.MCAP
    rows = server.query_messages(
        path=result["mcap"],
        sql_statement=(
            "SELECT COUNT(*) AS n, MIN(timestamp_seconds) AS lo, MAX(timestamp_seconds) AS hi "
            'FROM "message"'
        ),
        topic="message",
    )
    assert rows[0]["n"] == 21  # 15.0..25.0 inclusive at 0.5s cadence
    assert rows[0]["lo"] == 15.0
    assert rows[0]["hi"] == 25.0


def test_signal_validation_and_selection() -> None:
    picked = _export(signals=["message/vel"])
    assert picked["curves"] == ["message.vel"]

    with pytest.raises(ValueError, match="Unknown or non-numeric"):
        _export(signals=["message/no_such_field"])


def test_instructions_reference_both_files() -> None:
    result = _export()
    assert result["mcap"] in result["instructions"]
    assert result["layout"] in result["instructions"]
