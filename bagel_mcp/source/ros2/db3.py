"""Provide a data source for reading ROS2 sqlite3 bags."""

from __future__ import annotations

import pathlib

from bagel_mcp import ros_native
from bagel_mcp.di import module
from bagel_mcp.source.ros2 import base

FEATURE = "Reading ROS 2 .db3 bags"
rosbag2_py = ros_native.optional("rosbag2_py", feature=FEATURE)


class SourceFactory(base.SourceFactory):
    """A data source factory for reading from ROS2 sqlite3 bags."""

    def __init__(self, path: str) -> None:
        """Initialize the ROS2 sqlite3 bag data source factory.

        Args:
            path (str): The path to the ROS2 sqlite3 bag file or directory.

        """
        super().__init__(path)
        if pathlib.Path(path).is_dir() and self.compression_format == "zstd":
            raise ValueError(f"Directory contains .db3.zstd files is not supported: {path}")

    def build(self) -> rosbag2_py.SequentialReader:
        """Return a ROS2 SequentialReader object."""
        storage_options = rosbag2_py.StorageOptions(
            uri=str(self.path),
            storage_id="sqlite3",
        )
        converter_options = rosbag2_py.ConverterOptions(
            input_serialization_format="",
            output_serialization_format="",
        )
        reader = rosbag2_py.SequentialReader()
        reader.open(storage_options, converter_options)
        return reader


def register() -> None:
    """Register module for dependency injection (only where native ROS 2 is present)."""
    ros_native.require("rosbag2_py", feature=FEATURE)
    module.global_registry[__name__] = SourceFactory
