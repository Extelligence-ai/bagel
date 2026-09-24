"""Dry-run the anomaly screen over a recorded log: what would be flagged, and why.

No decision backend is involved and nothing is written. Use it before standing up an
`anomaly` gate to pick signals, thresholds and warm-up, the way `preview_pipeline`
is used before a reduction.
"""

import collections
from typing import Any

from src.pipeline import base, messages
from src.pipeline.decide import baseline, screen, summary

# A signal tripping the screen in more than this share of screened windows is not
# "anomalous"; it drifts by design or its threshold is wrong.
_NOISY_SHARE = 0.3
_WARMUP_SHARE = 0.5
# Epoch seconds well past any log; `inf` cannot be spliced into SQL.
_FAR_FUTURE = 1e13


class _Reader(messages.TopicMessageMixin):
    """Just the message access of a gate, without being one."""


def _advice(report: dict[str, Any]) -> list[str]:
    advice = []
    screened = report["screened_windows"]
    for label, count in sorted(report["by_signal"].items()):
        if screened and count / screened > _NOISY_SHARE:
            advice.append(
                f"'{label}' tripped the screen in {count} of {screened} screened windows: "
                "it likely drifts by design (a position, orientation or odometry state). "
                "Drop it from `signals`, or raise z_threshold if it is a rate that is "
                "genuinely this noisy."
            )
    for topic, count in sorted(report["by_topic"].items()):
        if screened and count / screened > _NOISY_SHARE:
            advice.append(
                f"'{topic}' read as a dropout in {count} of {screened} screened windows: it "
                "publishes slowly or only on events. Raise dropout_seconds above its period "
                "or leave it out of `topics`."
            )
    if report["windows"] and report["warmup_windows"] / report["windows"] >= _WARMUP_SHARE:
        advice.append(
            f"warm-up covered {report['warmup_windows']} of {report['windows']} windows: "
            "lower warmup_minutes for a log this short, or calibrate on a longer log."
        )
    return advice


def calibrate(  # noqa: PLR0913
    path: str,
    window_seconds: float,
    topics: list[str] | None = None,
    signals: list[str] | None = None,
    max_signals: int = 64,
    z_threshold: float = 3.0,
    dropout_seconds: float = 2.0,
    baseline_window_minutes: float = 30.0,
    warmup_minutes: float = 5.0,
    source_args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the on-robot screen over every window of a recorded log and report the flags.

    Windows are `window_seconds` long on a fixed grid from the start of the log (the
    gate itself fires on its cadence topic, so timestamps can differ slightly). Flagged
    windows are kept out of the rolling baseline, exactly as the gate does.

    Returns:
        ``windows``, ``warmup_windows``, ``screened_windows``, ``flagged`` (each with
        ``asof_seconds``, ``offset_seconds`` and the screen ``reasons``),
        ``flagged_fraction`` of screened windows, ``by_signal`` and ``by_topic`` flag
        counts, the watched ``signals``, and plain-language ``advice``.

    """
    if window_seconds <= 0 or window_seconds != int(window_seconds):
        raise ValueError("window_seconds must be a positive whole number of seconds")
    reader = _Reader()
    reader.setup(path, **(source_args or {}))
    lookback = base.Lookback(last=int(window_seconds), unit=base.Unit.SECOND)
    whole = reader.to_duckdb(topics=topics, asof_seconds=_FAR_FUTURE)
    start, end = whole.aggregate("min(timestamp_seconds), max(timestamp_seconds)").fetchall()[0]
    if start is None:
        raise ValueError(f"No messages found in {path!r} for topics {topics}")
    watched = summary.resolve_signals(whole, signals) if signals else summary.numeric_signals(whole)
    if len(watched) > max_signals:
        raise ValueError(
            f"{len(watched)} numeric signals exceed max_signals={max_signals}. Set 'topics' "
            "or 'signals' to the ones worth watching, or raise max_signals."
        )

    rolling = baseline.RollingBaseline(baseline_window_minutes, warmup_minutes)
    flagged: list[dict[str, Any]] = []
    by_signal: collections.Counter[str] = collections.Counter()
    by_topic: collections.Counter[str] = collections.Counter()
    windows = warmup = 0
    last_seen: dict[str, float] = {}
    asof = float(start)
    while asof <= end + 1e-9:
        windows += 1
        relation = reader.to_duckdb(topics=topics, asof_seconds=asof, lookback=lookback)
        window = summary.summarize(relation, watched)
        if rolling.ready(asof):
            reasons = screen.screen(
                window, rolling.stats(asof), asof, z_threshold, dropout_seconds, last_seen
            )
        else:
            warmup += 1
            reasons = []
        for topic, stats in window["topics"].items():
            if stats["last_seconds"] is not None:
                last_seen[topic] = stats["last_seconds"]
        if reasons:
            flagged.append(
                {"asof_seconds": asof, "offset_seconds": asof - start, "reasons": reasons}
            )
            by_signal.update({r["signal"] for r in reasons if "signal" in r})
            by_topic.update({r["topic"] for r in reasons if "topic" in r})
        else:
            rolling.add(window)
        asof += window_seconds

    screened = windows - warmup
    report: dict[str, Any] = {
        "path": path,
        "span_seconds": float(end) - float(start),
        "window_seconds": window_seconds,
        "signals": sorted(watched),
        "windows": windows,
        "warmup_windows": warmup,
        "screened_windows": screened,
        "flagged": flagged,
        "flagged_fraction": len(flagged) / screened if screened else 0.0,
        "by_signal": dict(by_signal),
        "by_topic": dict(by_topic),
    }
    report["advice"] = _advice(report)
    return report
