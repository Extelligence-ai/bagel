"""A message dataset for ASAM MDF (.mf4) files."""

import heapq
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from asammdf import MDF

from src.di import module
from src.message import base
from src.topic.automotive.mf4 import _channels, topic_names


class MessageDataset(base.MessageDataset):
    """A message dataset for ASAM MDF files.

    Topics are channel groups; each message is one sample cycle with a field per
    channel. Timestamps are absolute epoch seconds (the file's measurement start
    plus each sample's master time).
    """

    def _messages(
        self,
        data_source: MDF,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        names = topic_names(data_source)
        start_epoch = data_source.header.start_time.timestamp()

        def stream(topic: str) -> Iterator[tuple[float, int, str, dict[str, Any]]]:
            index = names[topic]
            master = data_source.get_master(index)
            columns = {
                name: data_source.get(name, group=index, raw=False).samples.tolist()
                for name in _channels(data_source, index)
            }
            for position, relative in enumerate(master.tolist()):
                timestamp = start_epoch + relative
                if start_seconds_inclusive is not None and timestamp < start_seconds_inclusive:
                    continue
                if end_seconds_inclusive is not None and timestamp > end_seconds_inclusive:
                    continue
                yield (
                    timestamp,
                    position,
                    topic,
                    {name: values[position] for name, values in columns.items()},
                )

        for timestamp, _, topic, message in heapq.merge(*(stream(topic) for topic in topics)):
            yield (topic, timestamp, message)

    def _to_json(self, message: dict[str, Any], struct: pa.StructType) -> dict[str, Any]:
        return message


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
