"""A base class for factories of ROS2 bag data source."""

import pathlib
from typing import Any

import yaml

from bagel_mcp import bags
from bagel_mcp.source import base, errors

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


def _missing_storage_files(path: pathlib.Path) -> list[str]:
    """Return the storage files metadata.yaml lists that are not on disk."""
    metadata_file = path / "metadata.yaml"
    if not path.is_dir() or not metadata_file.exists():
        return []
    metadata = yaml.safe_load(metadata_file.read_text(encoding="utf-8"))
    relative = metadata.get("rosbag2_bagfile_information", {}).get("relative_file_paths", [])
    return [str(path / name) for name in relative if not (path / name).exists()]


class SourceFactory(base.BoundedSourceFactory, base.FileBasedSourceFactory):
    """A base class for factories of ROS2 bag data source."""

    def __init__(self, path: str) -> None:
        """Initialize the ROS2 bag source factory.

        Args:
            path (str): The path to the ROS2 bag file or directory.

        """
        try:
            self._reader = bags.open_reader(path, ros_version=2)
        except bags.BagStorageError as error:
            # A bag directory whose storage files are gone: report them by name, as
            # validate_path() always did, instead of the backend's open failure.
            missing = _missing_storage_files(pathlib.Path(path))
            if missing:
                raise errors.MissingFilesError(missing) from error
            raise
        self._info = self._reader.info
        super().__init__(str(self._info.path))

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the ROS2 bag."""
        return {
            **self._bounded_metadata,
            **self._file_based_metadata,
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
        return self._info.message_count

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return bags.base.ros2_seconds(self._info.start_ns)

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return bags.base.ros2_seconds(self._info.end_ns)

    @property
    def size_bytes(self) -> int:
        """Return the bag size in bytes."""
        return self._info.size_bytes

    @property
    def version(self) -> str:
        """Return the bag version."""
        return self._info.version

    @property
    def storage_identifier(self) -> str:
        """Return the storage identifier."""
        return self._info.storage_identifier

    @property
    def compression_format(self) -> str:
        """Return the compression format."""
        return self._info.compression_format

    @property
    def compression_mode(self) -> str:
        """Return the compression mode."""
        return self._info.compression_mode

    @property
    def relative_file_paths(self) -> list[str]:
        """Return the relative file paths."""
        return list(self._info.relative_file_paths)

    @property
    def file_information(self) -> list[dict[str, Any]]:
        """Return the file information."""
        return [
            {
                "path": info.path,
                "message_count": info.message_count,
                "start_time_seconds": info.start_ns / SECOND,
                # microsecond precision, as rosbag2's timedelta reported it
                "duration_seconds": (info.duration_ns // 1_000) / 1e6,
            }
            for info in self._info.files
        ]

    @property
    def topic_information(self) -> list[dict[str, Any]]:
        """Return the topic information."""
        return [
            {
                "message_count": topic.message_count,
                "topic_metadata": {
                    "name": topic.name,
                    "type": topic.type_name,
                    "serialization_format": topic.serialization_format,
                },
            }
            for topic in self._info.topics
        ]

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS2 bag path."""
        files = (
            [self.path]
            if self.path.is_file()
            else [self.path / name for name in self.relative_file_paths]
        )
        missing_files = [f for f in files if not f.exists()]
        if missing_files:
            return False, errors.MissingFilesError([str(f) for f in missing_files])

        return True, None
