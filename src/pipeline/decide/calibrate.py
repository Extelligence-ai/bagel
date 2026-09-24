"""Dry-run the anomaly screen over a recorded log: what would be flagged, and why.

No decision backend is involved and nothing is written. Use it before standing up an
`anomaly` gate to pick signals, thresholds and warm-up, the way `preview_pipeline`
is used before a reduction.
"""

import collections
from typing import Any

from src.pipeline import base, messages
from src.pipeline.decide import baseline, screen, summary
from src.pipeline.gates.anomaly import _present_topics

# A signal tripping the screen in more than this share of screened windows is not
# "anomalous"; it drifts by design or its threshold is wrong.
_NOISY_SHARE = 0.3
_WARMUP_SHARE = 0.5
# Epoch seconds well past any log; `inf` cannot be spliced into SQL.
_FAR_FUTURE = 1e13


class _Reader(messages.TopicMessageMixin):
    """Just the message access of a gate, without being one."""


def _asof_timestamps(
    reader: _Reader, cadence_topic: str | None, start: float, end: float, every: float
) -> list[float]:
    """When the pipeline would fire: the same rule as `Pipeline._asof_timestamps`."""
    if cadence_topic is None:
        asofs = []
        asof = start
        while asof <= end + 1e-9:
            asofs.append(asof)
            asof += every
        return asofs
    relation = reader.to_duckdb(topics=[cadence_topic], asof_seconds=_FAR_FUTURE)
    rows = relation.project("timestamp_seconds").order("timestamp_seconds").fetchall()
    asofs = []
    last: float | None = None
    for (timestamp,) in rows:
        if last is None or timestamp - last >= every:
            asofs.append(float(timestamp))
            last = float(timestamp)
    return asofs


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


def _validate(  # noqa: PLR0913
    window_seconds: float,
    cadence_seconds: float | None,
    warmup_minutes: float,
    baseline_window_minutes: float,
    z_threshold: float,
    dropout_seconds: float,
    max_signals: int,
) -> float:
    """Check the arguments the way the gate and Pipeline.build would; return the cadence."""
    if window_seconds <= 0 or window_seconds != int(window_seconds):
        raise ValueError("window_seconds must be a positive whole number of seconds")
    if cadence_seconds is None:
        cadence_seconds = window_seconds
    if cadence_seconds <= 0 or cadence_seconds != int(cadence_seconds):
        raise ValueError("cadence_seconds must be a positive whole number of seconds")
    if z_threshold <= 0 or dropout_seconds <= 0:
        raise ValueError("z_threshold and dropout_seconds must be positive")
    if baseline_window_minutes <= 0 or warmup_minutes < 0:
        raise ValueError("baseline_window_minutes must be positive and warmup_minutes non-negative")
    if max_signals < 1:
        raise ValueError("max_signals must be at least 1")
    if warmup_minutes > baseline_window_minutes:
        raise ValueError(
            "warmup_minutes must not exceed baseline_window_minutes: the baseline forgets "
            "history faster than warm-up needs it and would never become ready"
        )
    return cadence_seconds


def _watched_signals(
    whole: Any,  # noqa: ANN401
    signals: list[str] | None,
    max_signals: int,
) -> summary.Signals:
    if signals is not None:  # [] is a choice: dropouts only
        watched = summary.resolve_signals(whole, signals)
    else:
        watched = summary.numeric_signals(whole)
    if len(watched) > max_signals:
        raise ValueError(
            f"{len(watched)} numeric signals exceed max_signals={max_signals}. Set 'topics' "
            "or 'signals' to the ones worth watching, or raise max_signals."
        )
    return watched


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
    cadence_topic: str | None = None,
    cadence_seconds: float | None = None,
    source_args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the on-robot screen over every window of a recorded log and report the flags.

    A screen-only preview: it assumes the gate runs on every fire (list it first among
    the gates), models `mode: screen` with the decision model confirming every flag,
    and reproduces `every: N seconds` cadences only. Windows are `window_seconds` long
    (the gate's lookback) and end every `cadence_seconds` (defaults to the window); with
    `cadence_topic`, at that topic's messages at least `cadence_seconds` apart, else on
    a fixed grid from the start of the log. Flagged windows are kept out of the rolling
    baseline, as the gate does.

    Returns:
        ``windows``, ``warmup_windows``, ``screened_windows``, ``flagged`` (each with
        ``asof_seconds``, ``offset_seconds`` and the screen ``reasons``),
        ``flagged_fraction`` of screened windows, ``by_signal`` and ``by_topic`` flag
        counts, the watched ``signals``, and plain-language ``advice``.

    """
    cadence_seconds = _validate(
        window_seconds,
        cadence_seconds,
        warmup_minutes,
        baseline_window_minutes,
        z_threshold,
        dropout_seconds,
        max_signals,
    )
    reader = _Reader()
    reader.setup(path, **(source_args or {}))
    lookback = base.Lookback(last=int(window_seconds), unit=base.Unit.SECOND)
    whole = reader.to_duckdb(topics=topics, asof_seconds=_FAR_FUTURE)
    start, end = whole.aggregate("min(timestamp_seconds), max(timestamp_seconds)").fetchall()[0]
    if start is None:
        raise ValueError(f"No messages found in {path!r} for topics {topics}")
    watched = _watched_signals(whole, signals, max_signals)

    rolling = baseline.RollingBaseline(baseline_window_minutes, warmup_minutes)
    flagged: list[dict[str, Any]] = []
    by_signal: collections.Counter[str] = collections.Counter()
    by_topic: collections.Counter[str] = collections.Counter()
    windows = warmup = 0
    last_seen: dict[str, float] = {}
    asofs: list[float] = []
    for asof in _asof_timestamps(reader, cadence_topic, float(start), float(end), cadence_seconds):
        asofs.append(asof)
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
        present = _present_topics(window, last_seen, asof, dropout_seconds)
        if reasons:
            flagged.append(
                {"asof_seconds": asof, "offset_seconds": asof - start, "reasons": reasons}
            )
            by_signal.update({r["signal"] for r in reasons if "signal" in r})
            by_topic.update({r["topic"] for r in reasons if "topic" in r})
        else:
            rolling.add(window, present)

    screened = windows - warmup
    report: dict[str, Any] = {
        "path": path,
        "span_seconds": float(end) - float(start),
        "window_seconds": window_seconds,
        "cadence_topic": cadence_topic,
        "cadence_seconds": cadence_seconds,
        "asof_offsets_seconds": [asof - float(start) for asof in asofs],
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
