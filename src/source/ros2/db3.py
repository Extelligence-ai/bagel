"""Provide a data source for reading ROS2 sqlite3 bags."""

import pathlib

import rosbag2_py

from src.source import base, errors
from src.source.ros2 import decompress


class ZstdDb3DirectoryError(Exception):
    """Raised when a ROS2 directory with .db3.zstd files is provided."""


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from ROS2 sqlite3 bags."""

    def __init__(self, path: str) -> None:
        """Initialize the ROS2 sqlite3 bag data source factory.

        Args:
            path (str): The path to the ROS2 sqlite3 bag file or directory.

        Notes:
            This factory will NOT handle ROS2 directories with .db3.zstd files.
            It only supports:
            - directory with metadata.yaml and .db3 files
            - .db3 file
            - .db3.zstd file

        """
        ros2bag_path = pathlib.Path(path)
        self._metadata = rosbag2_py.Info().read_metadata(path, "")
        if ros2bag_path.is_dir() and self._metadata.compression_format == "zstd":
            raise ZstdDb3DirectoryError(path)
        super().__init__(str(decompress.ros2bag(ros2bag_path)))

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

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS2 bag path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        db3_files = (
            [self.path]
            if self.path.is_file()
            else [self.path / file.path for file in self._metadata.files]
        )
        missing_files = [f for f in db3_files if not f.exists()]
        if missing_files:
            return False, errors.MissingFilesError([str(f) for f in missing_files])

        return True, None
