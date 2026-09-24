"""A cheap on-robot check of whether a window deviates from its baseline."""

# A constant baseline has std 0; compare against a small spread instead so any real
# change still scores as extreme without dividing by zero.
_MIN_RELATIVE_STD = 1e-3
_MIN_ABSOLUTE_STD = 1e-6


def screen(
    window: dict,
    baseline: dict,
    asof_seconds: float,
    z_threshold: float,
    dropout_seconds: float,
) -> list[dict]:
    """Return the reasons a window looks anomalous (empty if it looks normal).

    Reasons:
        ``{"kind": "z_score", "signal", "value", "z"}`` -- the window's most extreme
        value is more than `z_threshold` baseline standard deviations from the mean.
        ``{"kind": "dropout", "topic", "silent_seconds"}`` -- a topic that publishes in
        the baseline has been silent for more than `dropout_seconds` (None if it did
        not publish at all in the window).

    """
    reasons = []
    for label, normal in baseline["signals"].items():
        current = window["signals"].get(label)
        if not current or not current["count"]:
            continue
        mean = normal["mean"]
        spread = max(normal["std"], _MIN_RELATIVE_STD * abs(mean), _MIN_ABSOLUTE_STD)
        extreme = max((current["min"], current["max"]), key=lambda value: abs(value - mean))
        z = abs(extreme - mean) / spread
        if z > z_threshold:
            reasons.append({"kind": "z_score", "signal": label, "value": extreme, "z": z})
    for topic in baseline["topics"]:
        last = window["topics"].get(topic, {}).get("last_seconds")
        if last is None:
            reasons.append({"kind": "dropout", "topic": topic, "silent_seconds": None})
        elif asof_seconds - last > dropout_seconds:
            reasons.append(
                {"kind": "dropout", "topic": topic, "silent_seconds": asof_seconds - last}
            )
    return reasons
