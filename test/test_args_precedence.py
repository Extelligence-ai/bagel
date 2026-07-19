"""The explicit `path` parameter must always beat a `path` smuggled in via `args`.

Regression tests for the repo-wide sweep of `{**(args or {}), "path": path}` merge
order (originally flagged by review on the PlotJuggler export, then applied
everywhere): if `args` contained its own "path", the old order let it silently
override the path the caller actually asked for.
"""

from bagel import server

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"
POISONED = {
    "path": "./no/such/place",
    "timestamp_column": "t",
    "timestamp_format": "seconds",
}


def test_query_messages_explicit_path_wins() -> None:
    rows = server.query_messages(
        path=SAMPLE,
        sql_statement='SELECT COUNT(*) AS n FROM "message"',
        topic="message",
        args=POISONED,
    )
    assert rows[0]["n"] > 0


def test_read_loggings_explicit_path_wins() -> None:
    rows = server.read_loggings("data/sample/ros/log", args={"path": "./no/such/place"})
    assert len(rows) == 12


def test_describe_data_source_explicit_path_wins() -> None:
    result = server.describe_data_source(SAMPLE, args=POISONED)
    assert result  # resolving ./no/such/place would have raised
