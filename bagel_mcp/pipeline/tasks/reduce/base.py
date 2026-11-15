"""Shared logic for reduce tasks across data source formats."""

from bagel_mcp import query
from bagel_mcp.pipeline import windows
from bagel_mcp.settings import settings
from bagel_mcp.source.context import SourceContext


def check_options(  # noqa: PLR0913
    event_topic: str | None,
    predicate: str | None,
    pre_seconds: float,
    post_seconds: float,
    debounce_seconds: float,
    topics: list[str] | None,
    exclude_topics: list[str] | None,
) -> None:
    """Validate reduce options shared by every format, raising ValueError."""
    if (event_topic is None) != (predicate is None):
        raise ValueError(
            "'event_topic' and 'predicate' go together: set both to keep windows around "
            "events, or neither to keep the whole time range."
        )
    if topics is not None and exclude_topics is not None:
        raise ValueError("Set 'topics' (keep these) or 'exclude_topics' (drop these), not both.")
    if topics is not None and len(topics) == 0:
        raise ValueError("If 'topics' is specified, it must contain at least one topic name.")
    if exclude_topics is not None and len(exclude_topics) == 0:
        raise ValueError("If 'exclude_topics' is specified, it must name at least one topic.")
    if pre_seconds < 0 or post_seconds < 0:
        raise ValueError("'pre_seconds' and 'post_seconds' must be non-negative.")
    if debounce_seconds < 0:
        raise ValueError(f"'debounce_seconds' must be non-negative, got {debounce_seconds}.")


class ReduceMixin:
    """Compute the merged kept windows for an event predicate, and the kept topics.

    Expects the concrete task to also mix in ``TopicMessageMixin`` (for ``to_duckdb``)
    and to set ``_event_topic``, ``_predicate``, ``_pre_seconds``, ``_post_seconds``,
    ``_debounce_seconds``, ``_topics`` and ``_exclude_topics``. Without a predicate the
    whole time range is kept and only the topic selection applies.
    """

    def _kept_topics(self, available: list[str]) -> list[str]:
        """Return the topics to write, refusing unknown names and an empty result."""
        named = self._topics or self._exclude_topics or []
        if unknown := sorted(set(named) - set(available)):
            # A misspelled drop-list would silently keep the topic: refuse instead.
            raise ValueError(f"Topics not in the source: {unknown}; available: {available}")
        if self._exclude_topics is not None:
            kept = [topic for topic in available if topic not in set(self._exclude_topics)]
        else:
            kept = list(self._topics) if self._topics else list(available)
        if not kept:
            raise ValueError("Every topic would be dropped; refusing to write an empty log.")
        return kept

    def _kept_intervals(
        self, asof_seconds: float
    ) -> tuple[list[float], list[tuple[float | None, float | None]]]:
        """Return the detected event timestamps and the merged windows to keep.

        Event detection uses the decoded message path (``to_duckdb``); the concrete task
        is responsible for the raw write of the kept intervals. Without a predicate the
        single window is ``(None, None)``: every message, with no time filter at all
        (a bag's metadata start/end can disagree with its messages).
        """
        if self._predicate is None:
            return [], [(None, None)]
        bounds = SourceContext(self.factory, self.registry, self.dataset).bounds()
        relation = self.to_duckdb(topics=[self._event_topic], asof_seconds=asof_seconds)
        ts_column = settings.TIMESTAMP_SECONDS_COLUMN_NAME
        rows = windows.relation_rows(query.predicate_hits(relation, ts_column, self._predicate))
        plan = windows.plan_reduction(
            rows,
            self._pre_seconds,
            self._post_seconds,
            bounds[1] - bounds[0],
            self._debounce_seconds,
            bounds=bounds,
            ordered=True,
        )
        return plan["events"], plan["intervals"]
