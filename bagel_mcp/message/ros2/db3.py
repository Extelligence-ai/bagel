"""A message dataset for ROS2 sqlite3 bags."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from bagel_mcp import ros_native
from bagel_mcp.di import module
from bagel_mcp.message import base
from bagel_mcp.message.ros2 import convert

FEATURE = "Reading ROS 2 .db3 bag messages"
rosbag2_py = ros_native.optional("rosbag2_py", feature=FEATURE)
rclpy_serialization = ros_native.optional("rclpy.serialization", feature=FEATURE)
rosidl_utilities = ros_native.optional("rosidl_runtime_py.utilities", feature=FEATURE)

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
            deserialized_msg = rclpy_serialization.deserialize_message(
                serialized_msg, rosidl_utilities.get_message(type_names[topic])
            )
            yield topic, timestamp_seconds, deserialized_msg

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Cast a deserialized ROS2 message into a JSON-serializable dictionary."""
        return convert.to_json(message, struct)


def register() -> None:
    """Register module for dependency injection (only where native ROS 2 is present)."""
    ros_native.require("rosbag2_py", feature=FEATURE)
    module.global_registry[__name__] = MessageDataset
