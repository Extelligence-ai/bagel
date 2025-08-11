"""Base class for ROS2 bag readers."""

import logging
import pathlib
from collections.abc import Iterator
from typing import Any

import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from src.reader import reader
from src.reader.ros2 import metadata

LOGGING_MESSAGE_TYPE_NAME = "rcl_interfaces/msg/Log"


class LoggingMessage(reader.LoggingMessage):
    """Logging message in a ROS2 bag."""

    numeric_level: int
    topic: str
    name: str
    file: str
    function: str
    line: int

    def to_dict(self) -> dict[str, Any]:
        """Convert the logging message to a dictionary."""
        return {
            **super().to_dict(),
            "numeric_level": self.numeric_level,
            "topic": self.topic,
            "name": self.name,
            "file": self.file,
            "function": self.function,
            "line": self.line,
        }


class Ros2Reader(reader.Reader):
    """Base class for ROS2 bag readers."""

    def __init__(self, robolog_path: str | pathlib.Path, storage_id: str, use_cache: bool) -> None:
        """Initialize the Ros2Reader."""
        super().__init__(robolog_path, use_cache)

        self._storage_options = rosbag2_py.StorageOptions(uri=str(self.path), storage_id=storage_id)
        self._metadata = metadata.extract_metadata(self.path, self._storage_options.storage_id)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return robolog metadata as a JSON-serializable dictionary."""
        return self._metadata

    @property
    def start_seconds(self) -> float:
        """Return robolog start time in seconds."""
        return self._metadata["starting_time_seconds"]

    @property
    def end_seconds(self) -> float:
        """Return robolog end time in seconds."""
        return self._metadata["starting_time_seconds"] + self._metadata["duration_seconds"]

    @property
    def size_bytes(self) -> int:
        """Return robolog size in bytes."""
        return self.metadata["bag_size"]

    @property
    def topics(self) -> list[str]:
        """Return a list of topics in the robolog."""
        return list(
            set(
                topic_info["topic_metadata"]["name"]
                for topic_info in self.metadata["topics_with_message_count"]
            )
        )

    @property
    def type_names(self) -> dict[str, str]:
        """Return a mapping of topic names to their message type names."""
        return {
            topic_info["topic_metadata"]["name"]: topic_info["topic_metadata"]["type"]
            for topic_info in self.metadata["topics_with_message_count"]
        }

    @property
    def message_counts(self) -> dict[str, int]:
        """Return a mapping of topic names to their message counts."""
        return {
            topic_info["topic_metadata"]["name"]: topic_info["message_count"]
            for topic_info in self.metadata["topics_with_message_count"]
        }

    @property
    def logging_messages(self) -> Iterator[LoggingMessage]:
        """Iterate over logging messages in the robolog."""
        reader = rosbag2_py.SequentialReader()
        reader.open(self._storage_options, rosbag2_py.ConverterOptions("", ""))
        topics = [
            topic
            for topic, type_name in self.type_names.items()
            if type_name == LOGGING_MESSAGE_TYPE_NAME
        ]

        if not topics:
            logging.warning(
                "No logging messages found since %s topic type is not present.",
                LOGGING_MESSAGE_TYPE_NAME,
            )
            return

        reader.set_filter(rosbag2_py.StorageFilter(topics))

        while reader.has_next():
            topic, serialized_message, nanoseconds = reader.read_next()
            message = deserialize_message(
                serialized_message, get_message(LOGGING_MESSAGE_TYPE_NAME)
            )

            level = {
                10: "DEBUG",
                20: "INFO",
                30: "WARN",
                40: "ERROR",
                50: "FATAL",
            }.get(message.level, "UNKNOWN")

            yield LoggingMessage(
                robolog_id=self.robolog_id,
                timestamp_seconds=nanoseconds / 1e9,
                level=level,
                message=message.msg,
                numeric_level=message.level,
                topic=topic,
                name=message.name,
                file=message.file,
                function=message.function,
                line=message.line,
            )
