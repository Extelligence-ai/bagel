"""Provide a data source for reading ROS1 bags."""

import itertools
import statistics
from typing import Any

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.source import base, errors

FEATURE = "Reading ROS 1 .bag files"


class SourceFactory(base.BoundedSourceFactory, base.FileBasedSourceFactory):
    """A data source factory for reading from ROS1 bags."""

    def __init__(
        self,
        path: str,
        allow_unindexed: bool = True,
    ) -> None:
        """Initialize the ROS1 Bag data source factory.

        Args:
            path (str): Path to the .bag file.
            allow_unindexed (bool, optional): Accepted for compatibility; the pure-Python
                backend reads indexed bags (what `rosbag record` writes), so this has no
                effect there.

        """
        super().__init__(path)
        del allow_unindexed
        self._reader = bags.open_reader(path, ros_version=1)
        self._info = self._reader.info

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the ROS1 bag, shaped like `rosbag info --yaml`."""
        info = self._info
        topics = []
        for topic in sorted(info.topics, key=lambda t: t.name):
            entry: dict[str, Any] = {
                "topic": topic.name,
                "type": topic.type_name,
                "messages": topic.message_count,
            }
            frequency = self._frequency(topic.name, topic.message_count)
            if frequency is not None:
                entry["frequency"] = frequency
            topics.append(entry)
        types = sorted({(topic.type_name, topic.digest) for topic in info.topics})
        return {
            **self._bounded_metadata,
            **self._file_based_metadata,
            "version": float(info.version),
            "duration": round(self.end_seconds - self.start_seconds, 6),
            "start": round(self.start_seconds, 6),
            "end": round(self.end_seconds, 6),
            "size": info.size_bytes,
            "messages": info.message_count,
            "indexed": self.indexed,
            "compression": self.compression,
            "types": [{"type": type_name, "md5": digest} for type_name, digest in types],
            "topics": topics,
        }

    def _frequency(self, topic: str, count: int) -> float | None:
        """Publish rate as `rosbag info` reports it: 1 / median message spacing, 4 decimals."""
        if count < 2:  # noqa: PLR2004 -- a rate needs two stamps
            return None
        stamps = [
            bags.base.ros1_seconds(timestamp_ns)
            for _, timestamp_ns, _ in self._reader.raw_messages([topic], None, None)
        ]
        spacing = statistics.median(
            later - earlier for earlier, later in itertools.pairwise(stamps)
        )
        if spacing <= 0:
            return None
        return float(f"{1.0 / spacing:.4f}")

    @property
    def total_message_count(self) -> int:
        """Return the total number of messages."""
        return self._info.message_count

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return bags.base.ros1_seconds(self._info.start_ns)

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return bags.base.ros1_seconds(self._info.end_ns)

    @property
    def size_bytes(self) -> int:
        """Return the bag size in bytes."""
        return self._info.size_bytes

    @property
    def version(self) -> str:
        """Return the bag version."""
        return self._info.version

    @property
    def indexed(self) -> bool:
        """Return whether the bag is indexed (both backends read indexed bags)."""
        return True

    @property
    def compression(self) -> str:
        """Return the compression type."""
        return self._info.compression_format

    def build(self) -> bags.Reader:
        """Return the open bag reader."""
        return self._reader

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS1 bag file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".bag":
            return False, errors.InvalidFileExtensionError(".bag", self.path)

        return True, None


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 1)."""
    bags.require(FEATURE, ros_version=1)
    module.global_registry[__name__] = SourceFactory
