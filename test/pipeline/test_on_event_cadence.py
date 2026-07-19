"""Tests for the OnEvent rising-edge cadence and its config parsing."""

import duckdb
import pytest

from bagel.pipeline.base import Cadence, Frequency, Lookback, OnceAtEnd, OnEvent, Pipeline, Unit
from bagel.settings import settings

TS = settings.TIMESTAMP_SECONDS_COLUMN_NAME

# accel_x dips below -10 in two separate bursts (t=2..3 and t=7..8).
_ROWS = [
    (0.0, 0.0),
    (1.0, -2.0),
    (2.0, -12.0),
    (3.0, -14.0),
    (4.0, -3.0),
    (5.0, -1.0),
    (6.0, -2.0),
    (7.0, -11.0),
    (8.0, -13.0),
    (9.0, -4.0),
]


def _relation() -> duckdb.DuckDBPyRelation:
    values = ",".join(f"({t}, {a})" for t, a in _ROWS)
    return duckdb.sql(f"SELECT * FROM (VALUES {values}) AS t({TS}, accel_x)")


def _events(predicate: str, debounce: Lookback | None = None) -> list[float]:
    when = OnEvent(predicate=predicate, debounce=debounce)
    # `_event_timestamps` uses no instance state; bypass the heavy __init__.
    pipeline = Pipeline.__new__(Pipeline)
    return list(pipeline._event_timestamps(_relation(), when))


def test_rising_edge_fires_once_per_event() -> None:
    # A condition true across consecutive messages counts as a single event (its onset),
    # not one firing per message.
    assert _events("accel_x < -10") == [2.0, 7.0]


def test_debounce_coalesces_nearby_events() -> None:
    # The second event (t=7) is within 6s of the first (t=2) and is coalesced.
    assert _events("accel_x < -10", Lookback(last=6, unit=Unit.SECOND)) == [2.0]


def test_debounce_keeps_events_beyond_window() -> None:
    # With a 4s window the two events (t=2, t=7) are far enough apart to both fire.
    assert _events("accel_x < -10", Lookback(last=4, unit=Unit.SECOND)) == [2.0, 7.0]


def test_predicate_never_true_yields_nothing() -> None:
    assert _events("accel_x < -999") == []


def test_min_gap_seconds_defaults_to_zero() -> None:
    assert OnEvent(predicate="accel_x < -10").min_gap_seconds() == 0.0
    assert OnEvent(
        predicate="accel_x < -10", debounce=Lookback(last=3, unit=Unit.SECOND)
    ).min_gap_seconds() == 3.0


def test_cadence_build_parses_on_event() -> None:
    cadence = Cadence.build(
        {
            "topic": "/imu",
            "when": {
                "on_event": {
                    "predicate": "accel_x < -10",
                    "debounce": {"last": 2, "unit": "second"},
                }
            },
        }
    )
    assert isinstance(cadence.when, OnEvent)
    assert cadence.when.predicate == "accel_x < -10"
    assert cadence.when.min_gap_seconds() == 2.0


def test_cadence_build_on_event_without_debounce() -> None:
    cadence = Cadence.build(
        {"topic": "/imu", "when": {"on_event": {"predicate": "accel_x < -10"}}}
    )
    assert isinstance(cadence.when, OnEvent)
    assert cadence.when.debounce is None


@pytest.mark.parametrize(
    ("when_config", "expected_type"),
    [
        ("once_at_end", OnceAtEnd),
        ({"every": 30, "unit": "frame"}, Frequency),
    ],
)
def test_cadence_build_still_parses_existing_when_options(
    when_config: object, expected_type: type
) -> None:
    cadence = Cadence.build({"topic": "/camera", "when": when_config})
    assert isinstance(cadence.when, expected_type)
