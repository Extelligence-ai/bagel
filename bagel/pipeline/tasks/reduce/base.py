"""Shared logic for reduce tasks across data source formats."""

from bagel.pipeline import windows
from bagel.settings import settings


class ReduceMixin:
    """Compute the merged kept windows for an event predicate.

    Expects the concrete task to also mix in ``TopicMessageMixin`` (for ``to_duckdb``)
    and to set ``_event_topic``, ``_predicate``, ``_pre_seconds``, ``_post_seconds``,
    and ``_debounce_seconds``.
    """

    def _kept_intervals(self, asof_seconds: float) -> tuple[list[float], list[tuple[float, float]]]:
        """Return the detected event timestamps and the merged windows to keep.

        Event detection uses the decoded message path (``to_duckdb``); the concrete task
        is responsible for the raw write of the kept intervals.
        """
        relation = self.to_duckdb(topics=[self._event_topic], asof_seconds=asof_seconds)
        ts_column = settings.TIMESTAMP_SECONDS_COLUMN_NAME
        rows = relation.project(f"{ts_column} AS ts, ({self._predicate}) AS hit").fetchall()

        events = windows.rising_edges(rows, self._debounce_seconds)
        intervals = windows.merge_intervals(
            windows.event_windows(events, self._pre_seconds, self._post_seconds)
        )
        return events, intervals
