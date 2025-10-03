"""A message dataset for Bagel TopicSink."""

import heapq
from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from src.di import module
from src.message import base
from src.sink import reader


class MessageDataset(base.MessageDataset):
    """A message dataset for Bagel TopicSink."""

    def _messages(
        self,
        data_source: reader.TopicSinkReader,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_exclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        """Return an iterator of topic name, timestamp in seconds, and JSON message."""
        heap = []
        iterators = {}

        for topic in topics:
            iterators[topic] = data_source.reader(topic).messages()
            timestamp_seconds, msg = next(iterators[topic], (None, None))
            if timestamp_seconds is not None:
                heapq.heappush(heap, (timestamp_seconds, topic, msg))

        while heap:
            timestamp_seconds, topic, msg = heapq.heappop(heap)

            if end_seconds_exclusive is not None and timestamp_seconds >= end_seconds_exclusive:
                break

            if start_seconds_inclusive is not None and timestamp_seconds < start_seconds_inclusive:
                while tup := next(iterators[topic], None):
                    timestamp_seconds, msg = tup
                    if timestamp_seconds >= start_seconds_inclusive:
                        heapq.heappush(heap, (timestamp_seconds, topic, msg))
                        break
                continue

            yield (topic, timestamp_seconds, msg)

            tup = next(iterators[topic], None)
            if tup is not None:
                timestamp_seconds, msg = tup
                heapq.heappush(heap, (timestamp_seconds, topic, msg))

    def _to_json(self, message: dict[str, Any], struct: pa.StructType) -> dict[str, Any]:
        return message  # no-op, already JSON-serializable


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
