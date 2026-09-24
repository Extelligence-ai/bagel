"""Summarize a window of topic messages into compact per-signal statistics."""

import duckdb
from duckdb.typing import DuckDBPyType

from settings import settings

_NUMERIC_TYPES = {
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "hugeint",
    "utinyint",
    "usmallint",
    "uinteger",
    "ubigint",
    "uhugeint",
    "float",
    "double",
    "decimal",
}
# ROS headers carry timestamps that grow monotonically; as signals they would look
# anomalous in every window, so they are skipped unless selected explicitly.
_SKIPPED_FIELDS = {"header", "stamp"}
_STATS = ("count", "min", "max", "mean", "std")


def _numeric_leaves(label: str, dtype: DuckDBPyType) -> list[str]:
    """Return the dotted label of every numeric leaf under a column."""
    if dtype.id == "struct":
        return [
            leaf
            for child, child_type in dtype.children
            for leaf in _numeric_leaves(f"{label}.{child}", child_type)
        ]
    return [label] if dtype.id in _NUMERIC_TYPES else []


def _expression(label: str) -> str:
    column, *path = label.split(".")
    return f'"{column}"' + "".join(f"['{field}']" for field in path)


def summarize(relation: duckdb.DuckDBPyRelation, signals: list[str] | None = None) -> dict:
    """Summarize a window into per-signal and per-topic statistics.

    Args:
        relation: Messages in the window, one column per topic (see `TopicMessageMixin`).
        signals: Dotted signal labels to summarize, e.g. "/imu.linear_acceleration.x".
            If None, every numeric leaf except fields under `header`/`stamp`.

    Returns:
        ``{"window": {start_seconds, end_seconds, messages},
        "signals": {label: {count, min, max, mean, std}},
        "topics": {topic: {messages, last_seconds}}}``

    Raises:
        ValueError: If an explicit signal is not a numeric field of the relation.

    """
    ts = settings.TIMESTAMP_SECONDS_COLUMN_NAME
    topics = [column for column in relation.columns if column != ts]
    available = [
        leaf
        for column, dtype in zip(relation.columns, relation.types, strict=True)
        if column != ts
        for leaf in _numeric_leaves(column, dtype)
    ]
    if signals is None:
        labels = [label for label in available if not _SKIPPED_FIELDS & set(label.split(".")[1:])]
    else:
        if unknown := [label for label in signals if label not in available]:
            raise ValueError(f"Unknown numeric signals {unknown}; available: {available}")
        labels = list(signals)

    aggregates = ["count(*)", f"min({ts})::DOUBLE", f"max({ts})::DOUBLE"]
    for topic in topics:
        aggregates += [
            f'count("{topic}")',
            f'max({ts}) FILTER (WHERE "{topic}" IS NOT NULL)::DOUBLE',
        ]
    for label in labels:
        expression = _expression(label)
        aggregates += [
            f"count({expression})",
            f"min({expression})::DOUBLE",
            f"max({expression})::DOUBLE",
            f"avg({expression})::DOUBLE",
            f"stddev_pop({expression})::DOUBLE",
        ]
    messages, start, end, *values = relation.aggregate(", ".join(aggregates)).fetchall()[0]

    topic_stats = {}
    for topic in topics:
        count, last = values[:2]
        values = values[2:]
        topic_stats[topic] = {"messages": count, "last_seconds": last}
    signal_stats = {}
    for label in labels:
        signal_stats[label] = dict(zip(_STATS, values[: len(_STATS)], strict=True))
        values = values[len(_STATS) :]
    return {
        "window": {"start_seconds": start, "end_seconds": end, "messages": messages},
        "signals": signal_stats,
        "topics": topic_stats,
    }
