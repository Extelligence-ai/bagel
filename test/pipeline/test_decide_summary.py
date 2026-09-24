"""Tests for window summaries (`src.pipeline.decide.summary`)."""

import pathlib

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
        "/imu": {"messages": 3, "first_seconds": 10.0, "last_seconds": 12.0},
        "/motor": {"messages": 1, "first_seconds": 11.0, "last_seconds": 11.0},
    }


def test_empty_window() -> None:
    state = summary.summarize(_relation().filter(f"{TS} > 100"))
    assert state["window"]["messages"] == 0
    assert state["signals"]["/imu.accel.x"]["count"] == 0
    assert state["topics"]["/imu"] == {"messages": 0, "first_seconds": None, "last_seconds": None}


# --- robustness (from pre-release review) ------------------------------------------------


def _nan_relation() -> duckdb.DuckDBPyRelation:
    return duckdb.sql(
        f"""
        SELECT * FROM (VALUES
            (1.0, {{'v': 1.0, 'r': 'inf'::DOUBLE}}),
            (2.0, {{'v': 'nan'::DOUBLE, 'r': 'inf'::DOUBLE}}),
            (3.0, {{'v': 3.0, 'r': 'inf'::DOUBLE}})
        ) AS t({TS}, "/m")
        """
    )


def test_nan_and_inf_samples_are_left_out_of_the_statistics() -> None:
    # PX4 battery/estimator topics carry NaN by design; stddev_pop throws on them.
    signals = summary.summarize(_nan_relation())["signals"]
    assert signals["/m.v"] == {"count": 2, "min": 1.0, "max": 3.0, "mean": 2.0, "std": 1.0}


def test_a_signal_with_no_finite_samples_counts_zero() -> None:
    assert summary.summarize(_nan_relation())["signals"]["/m.r"]["count"] == 0


def test_field_names_containing_dots_and_quotes_are_summarized() -> None:
    # PX4 flattens nested fields to names like "previous.vx"; MQTT topics may hold dots.
    relation = duckdb.sql(
        f"""
        SELECT * FROM (VALUES
            (1.0, {{'previous.vx': 2.0, 'it''s': 4.0}}, {{'x': 1.0}}),
            (2.0, {{'previous.vx': 4.0, 'it''s': 6.0}}, {{'x': 3.0}})
        ) AS t({TS}, "/pos", "vehicle.speed")
        """
    )
    signals = summary.summarize(relation)["signals"]
    assert signals["/pos.previous.vx"]["mean"] == 3.0
    assert signals["/pos.it's"]["mean"] == 5.0
    assert signals["vehicle.speed.x"]["mean"] == 2.0


def test_explicit_signal_with_a_dotted_field_name_resolves() -> None:
    relation = duckdb.sql(
        f"SELECT * FROM (VALUES (1.0, {{'previous.vx': 2.0, 'y': 0.0}})) AS t({TS}, \"/pos\")"
    )
    state = summary.summarize(relation, signals=["/pos.previous.vx"])
    assert list(state["signals"]) == ["/pos.previous.vx"]


def test_integer_and_decimal_fields_are_summarized_as_doubles() -> None:
    relation = duckdb.sql(
        f"SELECT * FROM (VALUES (1.0, {{'i': 2, 'd': 1.5::DECIMAL(4,2)}})) AS t({TS}, \"/m\")"
    )
    signals = summary.summarize(relation)["signals"]
    assert signals["/m.i"]["mean"] == 2.0
    assert isinstance(signals["/m.d"]["mean"], float)


def test_summarizes_a_real_source_with_nan_and_dotted_fields(tmp_path: pathlib.Path) -> None:
    # The two crashes found in review: PX4 battery/estimator topics carry NaN by design,
    # and PX4 flattens nested fields to names like "previous.vx". A CSV source shows
    # both through the same TopicMessageMixin path in milliseconds instead of parsing
    # the whole bundled ULog.
    from src.pipeline import messages

    (tmp_path / "flight.csv").write_text(
        "t,previous.vx,batt\n0.0,1.0,nan\n0.5,3.0,2.0\n1.0,inf,4.0\n"
    )

    class Reader(messages.TopicMessageMixin):
        pass

    reader = Reader()
    reader.setup(path=str(tmp_path))
    relation = reader.to_duckdb(topics=None, asof_seconds=1e12)
    signals = summary.numeric_signals(relation)
    assert "message.previous.vx" in signals
    state = summary.summarize(relation, signals)
    assert state["signals"]["message.previous.vx"] == {
        "count": 2,
        "min": 1.0,
        "max": 3.0,
        "mean": 2.0,
        "std": 1.0,
    }
    assert state["signals"]["message.batt"]["count"] == 2


def test_absurdly_large_values_are_treated_as_missing() -> None:
    # Some drivers use DBL_MAX for "unknown"; squaring it overflows the baseline math and
    # DuckDB's stddev_pop raises on it.
    relation = duckdb.sql(
        f"SELECT * FROM (VALUES (1.0, {{'v': 1e300}}), (2.0, {{'v': 2.0}}), "
        f"(3.0, {{'v': 1.5e308}})) AS t({TS}, \"/m\")"
    )
    assert summary.summarize(relation)["signals"]["/m.v"] == {
        "count": 1,
        "min": 2.0,
        "max": 2.0,
        "mean": 2.0,
        "std": 0.0,
    }


def test_per_topic_first_seen() -> None:
    # A topic's earliest message matters for judging its period (Codex P1).
    assert summary.summarize(_relation())["topics"]["/motor"] == {
        "messages": 1,
        "first_seconds": 11.0,
        "last_seconds": 11.0,
    }
