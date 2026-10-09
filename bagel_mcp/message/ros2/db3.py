"""A message dataset for ROS2 sqlite3 bags."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.message import base
from bagel_mcp.message.ros2 import convert

FEATURE = "Reading ROS 2 .db3 bag messages"


class MessageDataset(base.MessageDataset):
    """A message dataset for ROS2 sqlite3 bags."""

    def _messages(
        self,
        data_source: bags.Reader,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, object]]:
        """Return an iterator of topic name, timestamp in seconds, and deserialized ROS2 message."""
        for topic, timestamp_ns, message in data_source.messages(
            topics, start_seconds_inclusive, end_seconds_inclusive
        ):
            yield topic, bags.base.ros2_seconds(timestamp_ns), message

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Cast a deserialized ROS2 message into a JSON-serializable dictionary."""
        return convert.to_json(message, struct)


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 2)."""
    bags.require(FEATURE, ros_version=2)
    module.global_registry[__name__] = MessageDataset
