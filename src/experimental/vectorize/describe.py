"""EXPERIMENTAL: turn a reduced event into a text description for embedding.

Edge-first and deterministic: no LLM call, just structured text built from the trigger
predicate, where the event happened, and any summary stats. That text is what the embedder
turns into a vector. Richer LLM-generated summaries are a later, optional layer.

Not part of a release. APIs will change.
"""

from typing import Any


def describe_event(
    predicate: str,
    *,
    event_topic: str | None = None,
    asset: str | None = None,
    site: str | None = None,
    stats: dict[str, Any] | None = None,
) -> str:
    """Build a deterministic, human-readable description of an event.

    Args:
        predicate (str): The condition that defined the event, e.g. "hard deceleration"
            or "linear_acceleration_x < -10".
        event_topic (str | None): The topic the event was detected on.
        asset (str | None): The asset (robot) the event came from.
        site (str | None): The deployment site.
        stats (dict[str, Any] | None): Summary stats for the window, e.g.
            {"peak_accel_x": -12.4, "duration_s": 2.1}. Rendered in insertion order so the
            output is stable.

    Returns:
        A single description string, e.g.
        "hard deceleration; on /imu on forklift_3 at warehouse; peak_accel_x -12.4, duration_s 2.1".

    """
    clauses: list[str] = [predicate.strip()]

    where: list[str] = []
    if event_topic:
        where.append(f"on {event_topic}")
    if asset:
        where.append(f"on {asset}")
    if site:
        where.append(f"at {site}")
    if where:
        clauses.append(" ".join(where))

    if stats:
        clauses.append(", ".join(f"{key} {value}" for key, value in stats.items()))

    return "; ".join(clauses)
