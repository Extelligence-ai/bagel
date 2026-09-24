"""Tests for window summaries (`src.pipeline.decide.summary`)."""

import duckdb
import pytest

from settings import settings
from src.pipeline.decide import summary

TS = settings.TIMESTAMP_SECONDS_COLUMN_NAME


def _relation() -> duckdb.DuckDBPyRelation:
    return duckdb.sql(
        f"""
        SELECT * FROM (VALUES
            (10.0, {{'header': {{'stamp': {{'sec': 10}}}}, 'accel': {{'x': -2.0, 'y': 0.5}},
                    'mode': 'auto'}}, NULL),
            (11.0, {{'header': {{'stamp': {{'sec': 11}}}}, 'accel': {{'x': -12.0, 'y': 0.0}},
                    'mode': 'auto'}}, {{'temp': 40.0}}),
            (12.0, {{'header': {{'stamp': {{'sec': 12}}}}, 'accel': {{'x': -4.0, 'y': 1.0}},
                    'mode': 'manual'}}, NULL)
        ) AS t({TS}, "/imu", "/motor")
        """
    )


def test_numeric_leaf_statistics() -> None:
    signals = summary.summarize(_relation())["signals"]
    x = signals["/imu.accel.x"]
    assert (x["count"], x["min"], x["max"], x["mean"]) == (3, -12.0, -2.0, -6.0)
    assert x["std"] == pytest.approx(4.3205, abs=1e-4)
    assert signals["/imu.accel.y"]["max"] == 1.0


def test_non_numeric_fields_are_skipped() -> None:
    assert "/imu.mode" not in summary.summarize(_relation())["signals"]


def test_header_and_stamp_fields_are_skipped_by_default() -> None:
    assert not [s for s in summary.summarize(_relation())["signals"] if "stamp" in s]


def test_explicit_signals_select_exactly_those() -> None:
    state = summary.summarize(_relation(), signals=["/imu.accel.x", "/imu.header.stamp.sec"])
    assert sorted(state["signals"]) == ["/imu.accel.x", "/imu.header.stamp.sec"]


def test_unknown_explicit_signal_is_rejected() -> None:
    with pytest.raises(ValueError, match="/imu.nope"):
        summary.summarize(_relation(), signals=["/imu.nope"])


def test_window_bounds() -> None:
    assert summary.summarize(_relation())["window"] == {
        "start_seconds": 10.0,
        "end_seconds": 12.0,
        "messages": 3,
    }


def test_per_topic_message_counts_and_last_seen() -> None:
    assert summary.summarize(_relation())["topics"] == {
        "/imu": {"messages": 3, "last_seconds": 12.0},
        "/motor": {"messages": 1, "last_seconds": 11.0},
    }


def test_empty_window() -> None:
    state = summary.summarize(_relation().filter(f"{TS} > 100"))
    assert state["window"]["messages"] == 0
    assert state["signals"]["/imu.accel.x"]["count"] == 0
    assert state["topics"]["/imu"] == {"messages": 0, "last_seconds": None}
