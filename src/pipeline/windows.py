"""Time-window utilities for event-driven data reduction.

Pure functions shared by the `OnEvent` cadence, the reduce task, and the
`preview_pipeline` audit. No I/O and no ROS dependencies -- everything here
operates on timestamps and intervals, so it is cheap to unit test.
"""

import math
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

import duckdb


def relation_rows(relation: duckdb.DuckDBPyRelation) -> Iterator[tuple]:
    """Consume ordered results in bounded batches without a whole-source row list."""
    while rows := relation.fetchmany(8192):
        yield from rows


def iter_rising_edges(
    samples: Iterable[tuple[float, Any]], min_gap_seconds: float = 0.0
) -> Iterator[float]:
    """Find edges in an ordered stream using constant state."""
    if not math.isfinite(min_gap_seconds) or min_gap_seconds < 0:
        raise ValueError("min_gap_seconds must be finite and non-negative")
    previous_hit = False
    last_edge: float | None = None
    for raw_timestamp, raw_hit in samples:
        hit = bool(raw_hit)
        timestamp = float(raw_timestamp)
        if (
            hit
            and not previous_hit
            and (last_edge is None or timestamp - last_edge >= min_gap_seconds)
        ):
            last_edge = timestamp
            yield timestamp
        previous_hit = hit


def rising_edges(samples: Iterable[tuple[float, Any]], min_gap_seconds: float = 0.0) -> list[float]:
    """Return timestamps where a boolean predicate transitions False -> True.

    A condition that stays true across consecutive samples produces a single edge
    at its onset (not one edge per sample). Consecutive edges closer together than
    ``min_gap_seconds`` are coalesced -- the later edge is dropped -- so bursty
    conditions do not produce a run of near-duplicate events.

    Args:
        samples: An iterable of ``(timestamp_seconds, hit)`` pairs. Order does not
            matter; the samples are sorted by timestamp internally. ``hit`` is
            coerced with ``bool`` (so SQL NULL / None counts as False).
        min_gap_seconds: Minimum seconds required between reported edges. 0 disables
            debouncing.

    Returns:
        Ascending list of rising-edge timestamps.

    """
    ordered = sorted(samples, key=lambda sample: sample[0])
    return list(iter_rising_edges(ordered, min_gap_seconds))


def event_windows(
    events: Iterable[float], pre_seconds: float, post_seconds: float
) -> list[tuple[float, float]]:
    """Build ``[event - pre, event + post]`` windows around each event timestamp.

    Raises:
        ValueError: If ``pre_seconds`` or ``post_seconds`` is negative.

    """
    if any(not math.isfinite(value) or value < 0 for value in (pre_seconds, post_seconds)):
        raise ValueError("pre_seconds and post_seconds must be finite and non-negative.")
    return [(float(event) - pre_seconds, float(event) + post_seconds) for event in events]


def merge_intervals(intervals: Iterable[tuple[float, float]]) -> list[tuple[float, float]]:
    """Union overlapping or touching intervals into disjoint, ascending intervals.

    Intervals whose start is greater than their end are dropped. Touching intervals
    (where one ends exactly where the next begins) are merged, so the result never
    contains adjacent pieces.
    """
    valid = sorted((float(start), float(end)) for start, end in intervals if start <= end)
    if not valid:
        return []
    merged = [valid[0]]
    for start, end in valid[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:  # overlaps or touches the current interval
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def total_duration(intervals: Sequence[tuple[float, float]]) -> float:
    """Return the summed length (seconds) of a set of intervals."""
    return sum(end - start for start, end in intervals)


def plan_reduction(  # noqa: PLR0913 -- preserve positional API; bounds/order are keyword-only
    samples: Iterable[tuple[float, Any]],
    pre_seconds: float,
    post_seconds: float,
    span_seconds: float,
    min_gap_seconds: float = 0.0,
    *,
    bounds: tuple[float, float] | None = None,
    ordered: bool = False,
) -> dict[str, Any]:
    """Compute the full reduction plan from predicate samples.

    This is the single entry point shared by ``preview_pipeline`` (which reports the
    numbers without writing) and the reduce task (which writes the kept intervals).

    Args:
        samples: ``(timestamp_seconds, hit)`` pairs from evaluating the predicate over
            the event topic.
        pre_seconds: Seconds to keep before each event.
        post_seconds: Seconds to keep after each event.
        span_seconds: Total time span of the source, used for the kept fraction.
        min_gap_seconds: Debounce window between consecutive events.
        bounds: Whole-source start/end; clip windows to data that actually exists.
        ordered: Stream pre-sorted samples without sorting them in Python.

    Returns:
        A dict with ``events`` (list of timestamps), ``intervals`` (merged kept
        windows), ``kept_seconds``, ``total_seconds``, and ``kept_fraction``.

    """
    events = (
        list(iter_rising_edges(samples, min_gap_seconds))
        if ordered
        else rising_edges(samples, min_gap_seconds)
    )
    intervals = merge_intervals(event_windows(events, pre_seconds, post_seconds))
    if bounds is not None:
        start, end = bounds
        if not all(math.isfinite(value) for value in bounds) or end < start:
            raise ValueError("Source bounds must be finite and ordered")
        intervals = merge_intervals((max(a, start), min(b, end)) for a, b in intervals)
        span_seconds = end - start
    kept_seconds = total_duration(intervals)
    return {
        "events": events,
        "intervals": intervals,
        "kept_seconds": kept_seconds,
        "total_seconds": span_seconds,
        "kept_fraction": (kept_seconds / span_seconds) if span_seconds > 0 else 0.0,
    }
