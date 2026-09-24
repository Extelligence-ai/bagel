"""A cheap on-robot check of whether a window deviates from its baseline."""

import math

# A constant baseline has std 0 (or float noise around it); compare against a small
# relative spread instead so any real change still scores as extreme.
_CONSTANT_STD = 1e-9
_FLOOR_RELATIVE = 1e-3
_FLOOR_ABSOLUTE = 1e-6


def _spread(normal: dict) -> float:
    mean, std = normal["mean"], normal["std"]
    if std > _CONSTANT_STD * max(1.0, abs(mean)):
        return std
    return max(_FLOOR_RELATIVE * abs(mean), _FLOOR_ABSOLUTE)


def extreme_threshold(z_threshold: float, samples: int) -> float:
    """Return the z a single sample must exceed, given how many samples were drawn.

    The most extreme of N normal samples sits near sqrt(2 ln N) std devs out by chance,
    so the per-sample rule adds that expectation to `z_threshold`; otherwise every
    window of a noisy 50 Hz signal would be flagged.
    """
    return z_threshold + math.sqrt(2.0 * math.log(max(samples, 1)))


def screen(  # noqa: PLR0913
    window: dict,
    baseline: dict,
    asof_seconds: float,
    z_threshold: float,
    dropout_seconds: float,
    last_seen: dict[str, float] | None = None,
) -> list[dict]:
    """Return the reasons a window looks anomalous (empty if it looks normal).

    Reasons:
        ``{"kind": "mean_shift", "signal", "value", "z"}`` -- the window's mean sits more
        than `z_threshold` baseline standard deviations from the baseline mean.
        ``{"kind": "z_score", "signal", "value", "z"}`` -- a single sample sits beyond
        `extreme_threshold(z_threshold, count)`.
        ``{"kind": "dropout", "topic", "silent_seconds"}`` -- a topic the baseline expects
        has been silent for more than `dropout_seconds` at the window's end. Silence is
        measured from the topic's last message, which `last_seen` carries across windows
        (a window shorter than the threshold is not a dropout on its own); None means the
        topic was never seen.

    """
    reasons = []
    for label, normal in baseline["signals"].items():
        current = window["signals"].get(label)
        if not current or not current["count"]:
            continue
        mean = normal["mean"]
        spread = _spread(normal)
        shift = abs(current["mean"] - mean) / spread
        if shift > z_threshold:
            reasons.append(
                {"kind": "mean_shift", "signal": label, "value": current["mean"], "z": shift}
            )
        extreme = max((current["min"], current["max"]), key=lambda value: abs(value - mean))
        z = abs(extreme - mean) / spread
        if z > extreme_threshold(z_threshold, current["count"]):
            reasons.append({"kind": "z_score", "signal": label, "value": extreme, "z": z})
    for topic in baseline["topics"]:
        last = window["topics"].get(topic, {}).get("last_seconds")
        if last is None and last_seen is not None:
            last = last_seen.get(topic)
        if last is None:
            reasons.append({"kind": "dropout", "topic": topic, "silent_seconds": None})
        elif asof_seconds - last > dropout_seconds:
            reasons.append(
                {"kind": "dropout", "topic": topic, "silent_seconds": asof_seconds - last}
            )
    return reasons
