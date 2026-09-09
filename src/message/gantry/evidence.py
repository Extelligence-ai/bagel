"""A message dataset for Gantry Bench evidence bundles."""

import heapq
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from src.di import module
from src.message import base
from src.source.gantry.evidence import EvidenceBundle
from src.topic.gantry.evidence import _topic


def _epoch(value: object) -> float | None:
    """ISO 8601 -> epoch seconds; timestamps without an offset are treated as UTC."""
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (ValueError, OverflowError, OSError):
        return None


class MessageDataset(base.MessageDataset):
    """A message dataset for Gantry Bench evidence bundles.

    Most of a bundle is not a timeseries: a finding or a ladder rung has no
    instant of its own. Rows are stamped so time semantics stay honest without
    inventing any:

    - ``events`` rows carry their own ``ts`` -- the one genuinely temporal
      topic.
    - ``gates`` rows carry their gate's ``started_at``.
    - Every other row carries the submission's ``created_at`` (falling back
      to the manifest's ``generated_at``), one constant instant, so time
      windows behave deterministically instead of drifting with the clock of
      whoever ran the query.
    """

    def _default_seconds(self, bundle: EvidenceBundle) -> float:
        manifest = bundle.manifest
        for value in (
            manifest.get("submission", {}).get("created_at"),
            manifest.get("generated_at"),
        ):
            seconds = _epoch(value)
            if seconds is not None:
                return seconds
        return 0.0

    def _stamp(self, topic: str, row: dict[str, Any], default: float) -> float:
        column = {"events": "ts", "gates": "started_at"}.get(topic)
        seconds = _epoch(row.get(column)) if column is not None else None
        return default if seconds is None else seconds

    def _messages(
        self,
        data_source: EvidenceBundle,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        """Return an iterator of topic name, timestamp in seconds, and message."""
        default = self._default_seconds(data_source)

        heap: list[tuple[float, int, str, dict[str, Any]]] = []
        tie = 0
        for topic in topics:
            for row in _topic(data_source, topic).to_pylist():
                seconds = self._stamp(topic, row, default)
                if start_seconds_inclusive is not None and seconds < start_seconds_inclusive:
                    continue
                if end_seconds_inclusive is not None and seconds > end_seconds_inclusive:
                    continue
                heapq.heappush(heap, (seconds, tie, topic, row))
                tie += 1

        while heap:
            seconds, _, topic, row = heapq.heappop(heap)
            yield (topic, seconds, row)

    def _to_json(self, message: dict[str, Any], struct: pa.StructType) -> dict[str, Any]:
        return message


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
