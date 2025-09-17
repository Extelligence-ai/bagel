"""A message dataset for ROS1 bags."""

from collections.abc import Iterator
from typing import Any

import genpy
import pyarrow as pa
import rosbag

from src import di
from src.message import base
from src.message.ros1 import convert


class MessageDataset(base.MessageDataset):
    """A message dataset for ROS1 bags."""

    def _messages(
        self,
        data_source: rosbag.Bag,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_exclusive: float | None,
    ) -> Iterator[str, float, object]:
        """Return an iterator of topic name, timestamp in seconds, and deserialized ROS1 message."""
        messages = data_source.read_messages(
            topics,
            genpy.Time.from_sec(start_seconds_inclusive) if start_seconds_inclusive else None,
            genpy.Time.from_sec(end_seconds_exclusive) if end_seconds_exclusive else None,
        )
        for topic, message, timestamp in messages:
            yield topic, timestamp.to_sec(), message

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Cast a deserialized ROS1 message into a JSON-serializable dictionary."""
        return convert.to_json(message, struct)


def register() -> None:
    """Register module for dependency injection."""
    di.module_registry[__name__] = MessageDataset
