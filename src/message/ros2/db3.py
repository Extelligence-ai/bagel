"""A message dataset for ROS2 sqlite3 bags."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from src.di import module
from src.message import base
from src.message.ros2 import convert

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class MessageDataset(base.MessageDataset):
    """A message dataset for ROS2 sqlite3 bags."""

    def _messages(
        self,
        data_source: rosbag2_py.SequentialReader,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, object]]:
        """Return an iterator of topic name, timestamp in seconds, and deserialized ROS2 message."""
        data_source.set_filter(rosbag2_py.StorageFilter(topics))
        if start_seconds_inclusive is not None:
            data_source.seek(int(start_seconds_inclusive * SECOND))
        type_names = {
            topic_metadata.name: topic_metadata.type
            for topic_metadata in data_source.get_all_topics_and_types()
        }
        while data_source.has_next():
            topic, serialized_msg, nanoseconds = data_source.read_next()
            timestamp_seconds = nanoseconds / SECOND
            if end_seconds_inclusive is not None and timestamp_seconds > end_seconds_inclusive:
                return
            deserialized_msg = deserialize_message(serialized_msg, get_message(type_names[topic]))
            yield topic, timestamp_seconds, deserialized_msg

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Cast a deserialized ROS2 message into a JSON-serializable dictionary."""
        return convert.to_json(message, struct)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
