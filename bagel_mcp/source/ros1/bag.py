"""Provide a data source for reading ROS1 bags."""

from __future__ import annotations

from typing import Any

import yaml

from bagel_mcp import ros_native
from bagel_mcp.di import module
from bagel_mcp.source import base, errors

FEATURE = "Reading ROS 1 .bag files"
rosbag = ros_native.optional("rosbag", feature=FEATURE)


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
            allow_unindexed (bool, optional): If True, allow opening unindexed bags.

        """
        super().__init__(path)
        self._bag = rosbag.Bag(f=path, mode="r", allow_unindexed=allow_unindexed)
        self._metadata = yaml.safe_load(self._bag._get_yaml_info())

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the ROS1 bag."""
        return {
            **self._bounded_metadata,
            **self._file_based_metadata,
            **self._metadata,
        }

    @property
    def total_message_count(self) -> int:
        """Return the total number of messages."""
        return self._metadata["messages"]

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return self._bag.get_start_time()

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return self._bag.get_end_time()

    @property
    def version(self) -> str:
        """Return the bag version."""
        return str(self._metadata["version"])

    @property
    def indexed(self) -> bool:
        """Return whether the bag is indexed."""
        return self._metadata["indexed"]

    @property
    def compression(self) -> str:
        """Return the compression type."""
        return self._metadata["compression"]

    def build(self) -> rosbag.Bag:
        """Return a ROS1 Bag object."""
        return self._bag

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
    """Register module for dependency injection (only where native ROS 1 is present)."""
    ros_native.require("rosbag", feature=FEATURE)
    module.global_registry[__name__] = SourceFactory
