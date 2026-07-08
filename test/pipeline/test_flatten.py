"""Tests for flattening struct topic relations into plot-ready scalar columns."""

import pathlib

import duckdb
import pytest

import server
from settings import settings
from src.pipeline import flatten

TS = settings.TIMESTAMP_SECONDS_COLUMN_NAME


def _relation(sql: str) -> duckdb.DuckDBPyRelation:
    return duckdb.sql(sql)


def test_flattens_nested_structs_with_slash_names() -> None:
    relation = _relation(
        f"""
        SELECT 1.5::DOUBLE AS "{TS}",
               struct_pack(
                   linear_acceleration := struct_pack(x := -12.0::DOUBLE, z := 9.8::DOUBLE),
                   frame := 'base'
               ) AS "/imu"
        """
    )
    flat = flatten.flatten(relation)
    assert flat.columns == [
        TS,
        "/imu/linear_acceleration/x",
        "/imu/linear_acceleration/z",
        "/imu/frame",
    ]
    assert flat.fetchone() == (1.5, -12.0, 9.8, "base")


def test_multiple_topics_and_timestamp_stays_first() -> None:
    relation = _relation(
        f"""
        SELECT 1.0 AS "{TS}",
               struct_pack(temp := -18.5) AS "freezer/1/status",
               struct_pack(pressure := 4.2) AS "plant/pump"
        """
    )
    flat = flatten.flatten(relation)
    assert flat.columns == [TS, "freezer/1/status/temp", "plant/pump/pressure"]


def test_non_scalar_leaves_are_skipped() -> None:
    relation = _relation(
        f"""
        SELECT 1.0 AS "{TS}",
               struct_pack(temp := 20.0, alarms := ['a', 'b']) AS "sensor"
        """
    )
    flat = flatten.flatten(relation)
    assert flat.columns == [TS, "sensor/temp"]


def test_end_to_end_flattened_csv_is_plot_ready(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    result = server.run_pipeline(
        {
            "name": "flat_export",
            "site": "test_site",
            "asset": "test_asset",
            "path": "./data/sample/pyarrow/csv/flight.csv",
            "allow_failure": False,
            "cadence": {"topic": "message", "when": "once_at_end"},
            "tasks": [
                {
                    "module": "src.pipeline.tasks.write_topics_to_file",
                    "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
                    "args": {
                        "topics": ["message"],
                        "output_format": "csv",
                        "flatten": True,
                    },
                }
            ],
        }
    )
    (artifact,) = result["artifacts"]
    header, first = pathlib.Path(artifact).read_text().splitlines()[:2]
    # One scalar column per signal -- what PlotJuggler's CSV loader expects.
    assert header == f"{TS},message/t,message/accel_x,message/vel"
    values = first.split(",")
    assert float(values[2]) == -0.5  # accel_x is numeric, not a struct blob


def test_end_to_end_flattened_parquet(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    result = server.run_pipeline(
        {
            "name": "flat_parquet",
            "site": "test_site",
            "asset": "test_asset",
            "path": "./data/sample/pyarrow/csv/flight.csv",
            "allow_failure": False,
            "cadence": {"topic": "message", "when": "once_at_end"},
            "tasks": [
                {
                    "module": "src.pipeline.tasks.write_topics_to_file",
                    "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
                    "args": {
                        "topics": ["message"],
                        "output_format": "parquet",
                        "flatten": True,
                    },
                }
            ],
        }
    )
    (artifact,) = result["artifacts"]
    table = duckdb.sql(f"SELECT * FROM '{artifact}'")  # noqa: S608
    assert "message/accel_x" in table.columns
    (minimum,) = duckdb.sql(
        f'SELECT MIN("message/accel_x") FROM \'{artifact}\''  # noqa: S608
    ).fetchone()
    assert minimum == -13.0
