"""Provide a data source for reading ROS2 sqlite3 bags."""

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.source.ros2 import base

FEATURE = "Reading ROS 2 .db3 bags"


class SourceFactory(base.SourceFactory):
    """A data source factory for reading from ROS2 sqlite3 bags.

    Accepts a bag directory (with ``metadata.yaml``), a single ``.db3`` file, or a single
    ``.db3.zstd`` file, including directories recorded with file- or message-level
    zstd compression.
    """

    def build(self) -> bags.Reader:
        """Return the open bag reader."""
        return self._reader


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 2)."""
    bags.require(FEATURE, ros_version=2)
    module.global_registry[__name__] = SourceFactory
