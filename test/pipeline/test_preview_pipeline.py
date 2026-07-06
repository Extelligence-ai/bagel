"""End-to-end test of the `preview_pipeline` MCP tool against a real CSV source.

Uses the pure-Python PyArrow CSV source (no ROS required), so the full path --
source factory -> DuckDB relation -> predicate evaluation -> window merge -> summary
-- runs in CI. The fixture `data/sample/pyarrow/csv/flight.csv` contains 60s of
telemetry with two sustained hard-braking events (accel_x < -10).
"""

import server

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"
# The CSV row is a DuckDB STRUCT named after the topic ("message"), so fields are
# accessed as message['field'] -- the same contract as query_messages / the sql gate.
PREDICATE = "message['accel_x'] < -10"
ARGS = {"timestamp_column": "t", "timestamp_format": "seconds"}


def test_preview_detects_two_braking_events() -> None:
    result = server.preview_pipeline(
        path=SAMPLE,
        event_topic="message",
        predicate=PREDICATE,
        pre_seconds=5.0,
        post_seconds=5.0,
        debounce_seconds=2.0,
        args=ARGS,
    )
    # Two sustained brakes -> two rising-edge events (onset only), not one per row.
    assert result["event_count"] == 2
    assert result["events"] == [20.0, 45.0]
    assert result["intervals"] == [
        {"start_seconds": 15.0, "end_seconds": 25.0},
        {"start_seconds": 40.0, "end_seconds": 50.0},
    ]
    assert result["kept_seconds"] == 20.0
    assert 0.33 < result["kept_fraction"] < 0.34


def test_preview_windows_merge_when_overlapping() -> None:
    # Wide windows make the two events' windows overlap into a single kept interval.
    result = server.preview_pipeline(
        path=SAMPLE,
        event_topic="message",
        predicate=PREDICATE,
        pre_seconds=20.0,
        post_seconds=20.0,
        args=ARGS,
    )
    assert result["event_count"] == 2
    assert len(result["intervals"]) == 1
    assert result["intervals"][0] == {"start_seconds": 0.0, "end_seconds": 65.0}


def test_preview_no_events_keeps_nothing() -> None:
    result = server.preview_pipeline(
        path=SAMPLE,
        event_topic="message",
        predicate="message['accel_x'] < -999",
        pre_seconds=5.0,
        post_seconds=5.0,
        args=ARGS,
    )
    assert result["event_count"] == 0
    assert result["intervals"] == []
    assert result["kept_seconds"] == 0.0
