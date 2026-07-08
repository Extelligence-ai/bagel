"""Streaming edge-recorder support for live event-driven reduction.

A live stream is unbounded, so the batch reduce (which scans a whole source) does not
apply. Instead, `LiveEventTrigger` decides *incrementally* which events to fire as
messages arrive, and -- crucially -- delays firing an event until its post-window has
elapsed, so the data after the event has already been buffered when the pipeline runs.

Both pieces here are pure (no ROS), so they are unit tested directly; the sink buffer
(`src/sink/buffer.py`) wires them onto the live message callback.
"""

from collections import deque

import duckdb
import pyarrow as pa

from settings import settings


class LiveEventTrigger:
    """Rising-edge trigger with a forward (post) window for live streams.

    Feed it ``(timestamp_seconds, hit)`` for each message on a monotonic stream. An event
    is the rising edge where ``hit`` goes False -> True (coalesced by ``debounce_seconds``).
    The event is not reported immediately: it fires only once a later message arrives at or
    after ``event_time + forward_seconds``, which guarantees the post-window data has been
    buffered. Call ``flush`` at stream end to fire any events still waiting on their window.
    """

    def __init__(self, forward_seconds: float = 0.0, debounce_seconds: float = 0.0) -> None:
        """Initialize the trigger.

        Args:
            forward_seconds: Seconds to wait after an event before firing it, so the
                post-window is captured. 0 fires as soon as the edge is seen.
            debounce_seconds: Minimum seconds between consecutive events; closer events are
                coalesced.

        Raises:
            ValueError: If either argument is negative.

        """
        if forward_seconds < 0 or debounce_seconds < 0:
            raise ValueError("forward_seconds and debounce_seconds must be non-negative.")
        self._forward_seconds = forward_seconds
        self._debounce_seconds = debounce_seconds
        self._previous_hit = False
        self._last_event_at: float | None = None
        self._pending: deque[float] = deque()

    def feed(self, timestamp_seconds: float, hit: object) -> list[float]:
        """Process one message and return the event timestamps ready to fire now.

        Args:
            timestamp_seconds: The message timestamp (monotonically non-decreasing).
            hit: Whether the predicate is true for this message (coerced with ``bool``).

        Returns:
            Event timestamps whose forward window has elapsed as of this message.

        """
        is_hit = bool(hit)
        if is_hit and not self._previous_hit:
            if (
                self._last_event_at is None
                or timestamp_seconds - self._last_event_at >= self._debounce_seconds
            ):
                self._last_event_at = timestamp_seconds
                self._pending.append(timestamp_seconds)
        self._previous_hit = is_hit

        ready: list[float] = []
        while self._pending and timestamp_seconds >= self._pending[0] + self._forward_seconds:
            ready.append(self._pending.popleft())
        return ready

    def flush(self) -> list[float]:
        """Return and clear all events still waiting on their forward window (stream end)."""
        ready = list(self._pending)
        self._pending.clear()
        return ready


def evaluate_predicate(topic: str, struct: pa.StructType, message: dict, predicate: str) -> bool:
    """Evaluate a SQL boolean predicate against a single live message.

    Builds a one-row DuckDB relation whose struct column is named after the topic -- the
    same contract as the ``gates.sql`` gate and ``query_messages`` -- so the predicate
    references fields as ``topic['field']``.

    Args:
        topic: The topic name (and struct column name).
        struct: The PyArrow struct type of the topic's messages.
        message: The decoded message as a dict.
        predicate: A SQL boolean expression over the topic's fields.

    Returns:
        True if the predicate holds for the message; False otherwise (including NULL).

    """
    table = pa.table(
        {
            settings.TIMESTAMP_SECONDS_COLUMN_NAME: pa.array([0.0], type=pa.float64()),
            topic: pa.array([message], type=struct),
        }
    )
    relation = duckdb.from_arrow(table)
    row = relation.project(f"({predicate}) AS hit").fetchone()
    return bool(row[0]) if row and row[0] is not None else False
