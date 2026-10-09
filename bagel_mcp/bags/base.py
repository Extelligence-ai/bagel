"""One internal interface over ROS bag files, whichever library reads or writes the bytes.

Sources, topic registries, message datasets and the bag-writing pipeline tasks talk
to a :class:`Reader` or :class:`Writer`; which library sits behind it is decided
once, in :mod:`bagel_mcp.bags`. Two backends implement the protocols:

- ``rosbags``: the pure-Python `rosbags <https://pypi.org/project/rosbags/>`_ library,
  what a pip install gets with the ``ros`` extra and what the service images use too.
- ``native``: ``rosbag`` (ROS 1) and ``rosbag2_py`` / ``rclpy`` (ROS 2), the apt-installed
  stacks of the service images. Kept for parity tests and as an escape hatch.

Timestamps cross the interface as integer nanoseconds; each ROS version's seconds
conversion lives with the dataset that needs it, so both backends produce
bit-identical floats.
"""

from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Iterator
from typing import Protocol

NANOSECONDS_PER_SECOND = 1_000_000_000


class BagSupportUnavailableError(ImportError):
    """No library that can read or write ROS bags is installed."""


class UnknownTopicError(KeyError):
    """The bag has no topic by that name."""


class BagStorageError(OSError):
    """The bag's storage files could not be opened (missing, truncated, unreadable)."""


@dataclasses.dataclass(frozen=True)
class TopicInfo:
    """One recorded topic.

    ``type_name`` uses the ROS version's own spelling: ``std_msgs/msg/String`` for
    ROS 2 and ``geometry_msgs/Twist`` for ROS 1. ``definition`` is the full-text message
    definition (dependencies appended under ``MSG:`` headers) when the bag carries
    one; ``digest`` is the ROS 1 md5sum or the ROS 2 type hash, empty if unknown.
    """

    name: str
    type_name: str
    message_count: int
    serialization_format: str = "cdr"
    definition: str | None = None
    digest: str = ""


@dataclasses.dataclass(frozen=True)
class FileInfo:
    """One storage file of a bag."""

    path: str
    message_count: int
    start_ns: int
    duration_ns: int


@dataclasses.dataclass(frozen=True)
class BagInfo:
    """What ``rosbag info`` / ``ros2 bag info`` report, for either ROS version."""

    path: pathlib.Path
    ros_version: int
    version: str
    storage_identifier: str
    compression_format: str
    compression_mode: str
    relative_file_paths: list[str]
    files: list[FileInfo]
    topics: list[TopicInfo]
    message_count: int
    start_ns: int
    end_ns: int
    size_bytes: int

    @property
    def topic_names(self) -> list[str]:
        """Return the topic names, sorted."""
        return sorted(topic.name for topic in self.topics)

    def topic(self, name: str) -> TopicInfo:
        """Return the topic named ``name``.

        Raises:
            UnknownTopicError: If the bag has no such topic.

        """
        for topic in self.topics:
            if topic.name == name:
                return topic
        raise UnknownTopicError(name)


class Reader(Protocol):
    """A bag open for reading."""

    info: BagInfo

    def definition(self, topic: str) -> str:
        """Return the full-text message definition of the topic's type."""

    def messages(
        self,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, int, object]]:
        """Yield ``(topic, timestamp_ns, deserialized message)`` in time order."""

    def raw_messages(
        self,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, int, bytes]]:
        """Yield ``(topic, timestamp_ns, serialized bytes)`` in time order."""

    def close(self) -> None:
        """Release the underlying files."""


class Writer(Protocol):
    """A bag open for writing; a context manager that closes on exit."""

    def add_topic(self, topic: TopicInfo) -> None:
        """Declare a topic before writing to it."""

    def write(self, topic: str, timestamp_ns: int, data: bytes) -> None:
        """Append one serialized message."""

    def close(self) -> None:
        """Finalize the bag (indexes, metadata)."""

    def __enter__(self) -> Writer:
        """Return self."""

    def __exit__(self, *exc_info: object) -> None:
        """Close the bag."""


def ros1_nanoseconds(seconds: float | None) -> int | None:
    """Convert a bound in seconds the way ``genpy.Time.from_sec`` does (whole, then fraction)."""
    if seconds is None:
        return None
    whole = int(seconds)
    return whole * NANOSECONDS_PER_SECOND + int((seconds - whole) * 1e9)


def ros1_seconds(timestamp_ns: int) -> float:
    """Convert nanoseconds to seconds the way ``genpy.Time.to_sec`` does (secs + nsecs/1e9)."""
    whole, fraction = divmod(timestamp_ns, NANOSECONDS_PER_SECOND)
    return whole + fraction / 1e9


def ros2_nanoseconds(seconds: float | None) -> int | None:
    """Convert a start bound in seconds the way the rosbag2 reader seek did (truncation)."""
    if seconds is None:
        return None
    return int(seconds * NANOSECONDS_PER_SECOND)


def ros2_seconds(timestamp_ns: int) -> float:
    """Convert nanoseconds to seconds the way the rosbag2 datasets always did."""
    return timestamp_ns / NANOSECONDS_PER_SECOND


def ros1_type_name(rosbags_type: str) -> str:
    """``geometry_msgs/msg/Twist`` (rosbags spelling) -> ``geometry_msgs/Twist`` (ROS 1)."""
    package, _, name = rosbags_type.partition("/msg/")
    return f"{package}/{name}" if name else rosbags_type


def rosbags_type_name(type_name: str) -> str:
    """``geometry_msgs/Twist`` -> ``geometry_msgs/msg/Twist``; ROS 2 names pass through."""
    if "/msg/" in type_name:
        return type_name
    package, _, name = type_name.partition("/")
    return f"{package}/msg/{name}"
