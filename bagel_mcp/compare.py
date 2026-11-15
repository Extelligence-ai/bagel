"""Compare two logs topic by topic: what changed in rates, gaps, values and diagnostics.

Code computes, the model narrates (#217): every number here is a DuckDB aggregate over
the two sources, and the result is ranked so the caller reports only what differs.
"""

import math
from typing import Any

from bagel_mcp.di import module
from bagel_mcp.di.types.base_module import BaseModule
from bagel_mcp.di.types.data_source import resolve
from bagel_mcp.logging.base import NoLoggingTopicsFoundError
from bagel_mcp.pipeline.decide import summary
from bagel_mcp.settings import settings
from bagel_mcp.source.context import SourceContext

# A shift of half a pooled standard deviation, or a spread that grows or shrinks by
# half, is the default bar for "changed".
DEFAULT_MIN_EFFECT = 0.5
_SPREAD_RATIO = 1.5
_RATE_RATIO = 1.25
_MAX_LOG_LINES = 20


def _topic_profile(source: SourceContext, topic: str) -> dict[str, Any]:
    relation = source.dataset.to_duckdb(source.factory, source.registry, [topic])
    stats = summary.summarize(relation)
    ts = '"' + settings.TIMESTAMP_SECONDS_COLUMN_NAME.replace('"', '""') + '"'
    (max_gap,) = relation.query(
        "messages",
        f"SELECT max(dt) FROM (SELECT {ts} - lag({ts}) OVER (ORDER BY {ts}) AS dt FROM messages)",  # noqa: S608 -- quoted internal column
    ).fetchone()
    counts = stats["topics"].get(topic, {})
    messages = counts.get("messages") or 0
    first, last = counts.get("first_seconds"), counts.get("last_seconds")
    span = (last - first) if first is not None and last is not None else 0.0
    return {
        "messages": messages,
        "rate_hz": (messages - 1) / span if messages > 1 and span > 0 else None,
        "max_gap_seconds": max_gap,
        "signals": stats["signals"],
    }


def _ratio(after: float | None, before: float | None) -> float | None:
    if after is None or before is None or before == 0:
        return None
    return after / before


def _signal_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any] | None:
    if not before.get("count") or not after.get("count"):
        return None
    mean_b, mean_a = before["mean"], after["mean"]
    std_b, std_a = before["std"] or 0.0, after["std"] or 0.0
    pooled = math.sqrt((std_b**2 + std_a**2) / 2)
    if pooled > 0:
        effect = (mean_a - mean_b) / pooled
    else:
        effect = 0.0 if mean_a == mean_b else math.copysign(math.inf, mean_a - mean_b)
    spread = _ratio(std_a, std_b) if std_b > 0 else (None if std_a == 0 else math.inf)
    spread_score = abs(math.log(spread)) / math.log(_SPREAD_RATIO) if spread else 0.0
    if spread == math.inf:
        spread_score = math.inf
    return {
        "baseline": before,
        "candidate": after,
        "mean_delta": mean_a - mean_b,
        "effect_size": effect,
        "spread_ratio": spread,
        "score": max(abs(effect), spread_score * DEFAULT_MIN_EFFECT),
    }


def _log_levels(source: SourceContext, path: str) -> tuple[dict[str, int], set[str]]:
    import_path = f"{BaseModule.LOGGING_DATASET.value}.{resolve(path).value}"
    try:
        dataset = module.provide(import_path, {})
        relation = dataset.to_duckdb(source.factory, source.registry, None, None)
    except NoLoggingTopicsFoundError:
        return {}, set()
    except ModuleNotFoundError as error:
        if not error.name or not import_path.startswith(error.name):
            raise
        return {}, set()  # the format has no diagnostic log (e.g. CSV)
    records = relation.to_df().to_dict(orient="records")
    levels: dict[str, int] = {}
    problems: set[str] = set()
    for record in records:
        message = record.get("message") or {}
        level = str(message.get("log_level") or "UNKNOWN").upper()
        levels[level] = levels.get(level, 0) + 1
        if level in {"WARN", "WARNING", "ERROR", "FATAL", "CRITICAL", "EMERGENCY"}:
            problems.add(f"{level}: {str(message.get('message', '')).strip()}")
    return levels, problems


def compare(  # noqa: PLR0913
    baseline_path: str,
    candidate_path: str,
    topics: list[str] | None = None,
    baseline_args: dict[str, Any] | None = None,
    candidate_args: dict[str, Any] | None = None,
    min_effect: float = DEFAULT_MIN_EFFECT,
    max_signals: int = 25,
) -> dict[str, Any]:
    """Diff the topics two logs share and rank what changed.

    Returns the topics found in only one log, per-topic rate and gap changes, the
    signals whose mean moved by at least `min_effect` pooled standard deviations or
    whose spread changed by half or more (ranked, at most `max_signals`), and the
    diagnostic log levels with WARN/ERROR lines that appear only in the candidate.
    """
    baseline = SourceContext.build(baseline_path, baseline_args)
    candidate = SourceContext.build(candidate_path, candidate_args)
    before_topics = set(baseline.registry.available_topics(baseline.factory.build()))
    after_topics = set(candidate.registry.available_topics(candidate.factory.build()))
    shared = sorted(before_topics & after_topics)
    if topics is not None:
        if unknown := sorted(set(topics) - set(shared)):
            raise ValueError(f"Topics not present in both logs: {unknown}; shared: {shared}")
        shared = sorted(topics)

    topic_changes, signal_changes = [], []
    unchanged_signals = 0
    for topic in shared:
        before = _topic_profile(baseline, topic)
        after = _topic_profile(candidate, topic)
        rate_ratio = _ratio(after["rate_hz"], before["rate_hz"])
        topic_changes.append(
            {
                "topic": topic,
                "messages": [before["messages"], after["messages"]],
                "rate_hz": [before["rate_hz"], after["rate_hz"]],
                "max_gap_seconds": [before["max_gap_seconds"], after["max_gap_seconds"]],
                "rate_changed": rate_ratio is not None
                and not 1 / _RATE_RATIO <= rate_ratio <= _RATE_RATIO,
            }
        )
        for label in sorted(set(before["signals"]) & set(after["signals"])):
            delta = _signal_delta(before["signals"][label], after["signals"][label])
            if delta is None:
                continue
            if delta["score"] >= min_effect:
                signal_changes.append({"signal": label, **delta})
            else:
                unchanged_signals += 1

    signal_changes.sort(key=lambda change: change["score"], reverse=True)
    before_levels, before_problems = _log_levels(baseline, baseline_path)
    after_levels, after_problems = _log_levels(candidate, candidate_path)
    new_problems = sorted(after_problems - before_problems)
    return {
        "topics_only_in_baseline": sorted(before_topics - after_topics),
        "topics_only_in_candidate": sorted(after_topics - before_topics),
        "topics_compared": len(shared),
        "topics": [change for change in topic_changes if change["rate_changed"]],
        "topics_unchanged_rate": [c["topic"] for c in topic_changes if not c["rate_changed"]],
        "signals_changed": signal_changes[:max_signals],
        "signals_changed_total": len(signal_changes),
        "signals_unchanged": unchanged_signals,
        "log_levels": {
            level: [before_levels.get(level, 0), after_levels.get(level, 0)]
            for level in sorted(set(before_levels) | set(after_levels))
        },
        "new_warnings_and_errors": new_problems[:_MAX_LOG_LINES],
        "new_warnings_and_errors_total": len(new_problems),
    }
