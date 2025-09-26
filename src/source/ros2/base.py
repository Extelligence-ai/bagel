"""A base class for factories of ROS2 bag data source."""

import pathlib
from typing import Any

import rosbag2_py

from src.source import base, errors
from src.source.ros2 import decompress

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class SourceFactory(base.FileBasedSourceFactory):
    """A base class for factories of ROS2 bag data source."""

    def __init__(self, path: str) -> None:
        """Initialize the ROS2 bag source factory.

        Args:
            path (str): The path to the ROS2 bag file or directory.

        """
        path = str(decompress.ros2bag(pathlib.Path(path)))
        self._metadata = rosbag2_py.Info().read_metadata(path, "")
        super().__init__(path)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the ROS2 bag."""
        return {
            **super().metadata,
            "version": self.version,
            "storage_identifier": self.storage_identifier,
            "compression_format": self.compression_format,
            "compression_mode": self.compression_mode,
            "relative_file_paths": self.relative_file_paths,
            "file_information": self.file_information,
            "topic_information": self.topic_information,
        }

    @property
    def total_message_count(self) -> int:
        """Return the total number of messages."""
        return self._metadata.message_count

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return float(self._metadata.starting_time.nanoseconds / SECOND)

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return (
            float(self._metadata.starting_time.nanoseconds + self._metadata.duration.nanoseconds)
            / SECOND
        )

    @property
    def size_bytes(self) -> int:
        """Return the bag size in bytes."""
        return self._metadata.bag_size

    @property
    def version(self) -> str:
        """Return the bag version."""
        return str(self._metadata.version)

    @property
    def storage_identifier(self) -> str:
        """Return the storage identifier."""
        return self._metadata.storage_identifier

    @property
    def compression_format(self) -> str:
        """Return the compression format."""
        return self._metadata.compression_format

    @property
    def compression_mode(self) -> str:
        """Return the compression mode."""
        return self._metadata.compression_mode

    @property
    def relative_file_paths(self) -> list[str]:
        """Return the relative file paths."""
        return self._metadata.relative_file_paths

    @property
    def file_information(self) -> list[dict[str, Any]]:
        """Return the file information."""
        return [
            {
                "path": info.path,
                "message_count": info.message_count,
                "start_time_seconds": info.starting_time.nanoseconds / SECOND,
                "duration_seconds": info.duration.total_seconds(),
            }
            for info in self._metadata.files
        ]

    @property
    def topic_information(self) -> list[dict[str, Any]]:
        """Return the topic information."""
        return [
            {
                "message_count": info.message_count,
                "topic_metadata": {
                    "name": info.topic_metadata.name,
                    "type": info.topic_metadata.type,
                    "type_description_hash": info.topic_metadata.type_description_hash,
                    "serialization_format": info.topic_metadata.serialization_format,
                },
            }
            for info in self._metadata.topics_with_message_count
        ]

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS2 bag path."""
        files = (
            [self.path]
            if self.path.is_file()
            else [self.path / info["path"] for info in self.file_information]
        )
        missing_files = [f for f in files if not f.exists()]
        if missing_files:
            return False, errors.MissingFilesError([str(f) for f in missing_files])

        return True, None
