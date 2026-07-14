"""Provide a data source for reading plain-text ROS log files.

This reads the log files ROS dumps to disk (e.g., ~/.ros/log) directly, without
spinning up a bag: a lightweight way to inspect INFO/WARN/ERROR messages.
"""

import pathlib
from typing import Any

from src.di import module
from src.source import base, errors
from src.source.ros.parse import LogRecord, parse_file


class RosLogSource:
    """A parsed view over one or more ROS text log files."""

    def __init__(self, files: list[pathlib.Path]) -> None:
        """Initialize the source with the log files to read.

        Args:
            files (list[pathlib.Path]): The ROS text log files, in a stable order.

        """
        self.files = files
        self._records: list[LogRecord] | None = None

    @property
    def records(self) -> list[LogRecord]:
        """All log records across the files, sorted by timestamp."""
        if self._records is None:
            records = [record for file in self.files for record in parse_file(file)]
            records.sort(key=lambda record: record.timestamp_seconds)
            self._records = records
        return self._records


class SourceFactory(base.BoundedSourceFactory, base.FileBasedSourceFactory):
    """A data source factory for reading plain-text ROS log files."""

    def __init__(self, path: str) -> None:
        """Initialize the ROS log data source factory.

        Args:
            path (str): Path to a ROS .log file, or a directory containing them
                (such as ~/.ros/log or one of its per-run subdirectories).

        """
        super().__init__(path)
        if self.path.is_file():
            self._files = [self.path]
        else:
            self._files = sorted(self.path.glob("**/*.log"))
        self._source = RosLogSource(self._files)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the log files."""
        return {
            **self._bounded_metadata,
            **self._file_based_metadata,
            "files": [str(file) for file in self._files],
        }

    @property
    def total_message_count(self) -> int:
        """Return the total number of log records."""
        return len(self._source.records)

    @property
    def start_seconds(self) -> float:
        """Return the timestamp of the first log record in seconds."""
        records = self._source.records
        return records[0].timestamp_seconds if records else 0.0

    @property
    def end_seconds(self) -> float:
        """Return the timestamp of the last log record in seconds."""
        records = self._source.records
        return records[-1].timestamp_seconds if records else 0.0

    def build(self) -> RosLogSource:
        """Return the parsed log source."""
        return self._source

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate that the path is a .log file or a directory containing them."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if self.path.is_file() and self.path.suffix != ".log":
            return False, errors.InvalidFileExtensionError(".log", self.path)

        if self.path.is_dir() and not any(self.path.glob("**/*.log")):
            return False, ValueError(f"No .log files found at {self.path}")

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
