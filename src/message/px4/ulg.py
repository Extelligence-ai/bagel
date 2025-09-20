"""A message dataset for PX4 ULogs."""

import heapq
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from pyulog import core

from src.di import module
from src.message import base

MILLISECOND = 1
SECOND = 1_000 * MILLISECOND


class MessageDataset(base.MessageDataset):
    """A message dataset for PX4 ULogs."""

    def _messages(
        self,
        data_source: core.ULog,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_exclusive: float | None,
    ) -> Iterator[str, float, dict[str, Any]]:
        """Return an iterator of topic name, timestamp in seconds, and message dictionary."""
        timestamps_and_indices, datasets = {}, {}
        for topic in topics:
            type_name, multi_id = topic.rsplit("_", 1)
            dataset = data_source.get_dataset(type_name, int(multi_id))
            ts_field_name = dataset.field_data[dataset.timestamp_idx].field_name

            timestamps = dataset.data[ts_field_name] / SECOND
            condition = [True] * len(timestamps)
            if start_seconds_inclusive is not None:
                condition &= timestamps >= start_seconds_inclusive
            if end_seconds_exclusive is not None:
                condition &= timestamps < end_seconds_exclusive

            timestamps_and_indices[topic] = iter(
                [(timestamps[i], i) for i, c in enumerate(condition) if c]
            )
            datasets[topic] = dataset

        heap = []
        for topic, ts_and_i in timestamps_and_indices.items():
            timestamp_seconds, i = next(ts_and_i)
            heapq.heappush(heap, (timestamp_seconds, topic, i))

        while heap:
            timestamp_seconds, topic, i = heapq.heappop(heap)

            message = {}
            for field, values in datasets[topic].data.items():
                message[field] = values[i]

            yield topic, timestamp_seconds, message

            try:
                next_timestamp_seconds, next_i = next(timestamps_and_indices[topic])
                heapq.heappush(heap, (next_timestamp_seconds, topic, next_i))
            except StopIteration:
                pass

    def _to_json(self, message: dict[str, Any], struct: pa.StructType) -> dict[str, Any]:
        """Cast a message dictionary into a JSON-serializable dictionary."""
        return message  # identity function since message is already a dict


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
