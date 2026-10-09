"""A message dataset for ROS1 bags."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from bagel_mcp import ros_native
from bagel_mcp.di import module
from bagel_mcp.message import base
from bagel_mcp.message.ros1 import convert

FEATURE = "Reading ROS 1 .bag messages"
rosbag = ros_native.optional("rosbag", feature=FEATURE)
genpy = ros_native.optional("genpy", feature=FEATURE)


class MessageDataset(base.MessageDataset):
    """A message dataset for ROS1 bags."""

    def _messages(
        self,
        data_source: rosbag.Bag,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, object]]:
        """Return an iterator of topic name, timestamp in seconds, and deserialized ROS1 message."""
        messages = data_source.read_messages(
            topics,
            genpy.Time.from_sec(start_seconds_inclusive) if start_seconds_inclusive else None,
            genpy.Time.from_sec(end_seconds_inclusive) if end_seconds_inclusive else None,
        )
        for topic, message, timestamp in messages:
            yield topic, timestamp.to_sec(), message

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Cast a deserialized ROS1 message into a JSON-serializable dictionary."""
        return convert.to_json(message, struct)


def register() -> None:
    """Register module for dependency injection (only where native ROS 1 is present)."""
    ros_native.require("rosbag", feature=FEATURE)
    module.global_registry[__name__] = MessageDataset
