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
_MAX_MAGNITUDE = 1e150

# label -> (topic column, struct field path). Field names may themselves contain dots
# (PX4 flattens nested fields to "previous.vx"), so labels are never re-split.
Signals = dict[str, tuple[str, tuple[str, ...]]]


def _numeric_leaves(column: str, path: tuple[str, ...], dtype: DuckDBPyType) -> Signals:
    if dtype.id == "struct":
        leaves: Signals = {}
        for child, child_type in dtype.children:
            leaves.update(_numeric_leaves(column, (*path, child), child_type))
        return leaves
    if dtype.id in _NUMERIC_TYPES:
        return {".".join((column, *path)): (column, path)}
    return {}


def numeric_signals(relation: duckdb.DuckDBPyRelation, include_headers: bool = False) -> Signals:
    """Return every numeric leaf of the relation, keyed by dotted label.

    A schema walk only: nothing is scanned. Fields under `header`/`stamp` are left out
    unless `include_headers` is set.
    """
    ts = settings.TIMESTAMP_SECONDS_COLUMN_NAME
    signals: Signals = {}
    for column, dtype in zip(relation.columns, relation.types, strict=True):
        if column == ts:
            continue
        for label, (_, path) in _numeric_leaves(column, (), dtype).items():
            if include_headers or not _SKIPPED_FIELDS & set(path):
                signals[label] = (column, path)
    return signals


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(name: str) -> str:
    return "'" + name.replace("'", "''") + "'"


def _expression(column: str, path: tuple[str, ...]) -> str:
    """Return a DOUBLE expression for the leaf, NULL where the sample is NaN or infinite.

    DuckDB's stddev_pop raises when NaN/Inf mix with finite values, and PX4/ROS topics
    carry both by design, so non-finite samples are dropped rather than propagated.
    """
    value = _quote_identifier(column) + "".join(f"[{_quote_literal(f)}]" for f in path)
    double = f"{value}::DOUBLE"
    # DBL_MAX-style "unknown" markers (1e308) survive isfinite but overflow when squared.
    return (
        f"(CASE WHEN isfinite({double}) AND abs({double}) < {_MAX_MAGNITUDE:g} THEN {double} END)"
    )


def resolve_signals(relation: duckdb.DuckDBPyRelation, labels: list[str]) -> Signals:
    """Map explicit dotted labels to their columns/paths, or raise on unknown ones."""
    available = numeric_signals(relation, include_headers=True)
    if unknown := [label for label in labels if label not in available]:
        raise ValueError(f"Unknown numeric signals {unknown}; available: {sorted(available)}")
    return {label: available[label] for label in labels}


def summarize(
    relation: duckdb.DuckDBPyRelation, signals: Signals | list[str] | None = None
) -> dict:
    """Summarize a window into per-signal and per-topic statistics.

    Args:
        relation: Messages in the window, one column per topic (see `TopicMessageMixin`).
        signals: Which signals to summarize: a `numeric_signals()` mapping, a list of
            dotted labels, or None for every numeric leaf except `header`/`stamp` fields.
            Callers that watch many topics should pick a bounded set: each signal adds
            five aggregates to one DuckDB statement.

    Returns:
        ``{"window": {start_seconds, end_seconds, messages},
        "signals": {label: {count, min, max, mean, std}},
        "topics": {topic: {messages, last_seconds}}}``

    Raises:
        ValueError: If an explicit label is not a numeric field of the relation.

    """
    ts = settings.TIMESTAMP_SECONDS_COLUMN_NAME
    topics = [column for column in relation.columns if column != ts]
    if signals is None:
        chosen = numeric_signals(relation)
    elif isinstance(signals, dict):
        chosen = signals
    else:
        chosen = resolve_signals(relation, signals)

    aggregates = ["count(*)", f"min({ts})::DOUBLE", f"max({ts})::DOUBLE"]
    for topic in topics:
        quoted = _quote_identifier(topic)
        aggregates += [f"count({quoted})", f"max({ts}) FILTER (WHERE {quoted} IS NOT NULL)::DOUBLE"]
    for column, path in chosen.values():
        expression = _expression(column, path)
        aggregates += [
            f"count({expression})",
            f"min({expression})",
            f"max({expression})",
            f"avg({expression})",
            f"stddev_pop({expression})",
        ]
    messages, start, end, *values = relation.aggregate(", ".join(aggregates)).fetchall()[0]

    topic_stats = {}
    for topic in topics:
        count, last = values[:2]
        values = values[2:]
        topic_stats[topic] = {"messages": count, "last_seconds": last}
    signal_stats = {}
    for label in chosen:
        signal_stats[label] = dict(zip(_STATS, values[: len(_STATS)], strict=True))
        values = values[len(_STATS) :]
    return {
        "window": {"start_seconds": start, "end_seconds": end, "messages": messages},
        "signals": signal_stats,
        "topics": topic_stats,
    }
