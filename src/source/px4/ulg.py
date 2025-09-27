"""Provide a data source for reading PX4 ULogs."""

from typing import Any

from pyulog import core

from src.di import module
from src.source import base, errors

MICROSECOND = 1
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class SourceFactory(base.FileBasedSourceFactory):
    """A data source factory for reading from PX4 ULogs."""

    def __init__(
        self,
        path: str,
        message_name_filter_list: list[str] | None = None,
        disable_str_exceptions: bool = True,
    ) -> None:
        """Initialize the PX4 ULog data source factory.

        Args:
            path (str): Path to the .ulg file.
            message_name_filter_list (list[str] | None, optional): A list of message names to load.
                If None, load everything.
            disable_str_exceptions (bool, optional): If True, ignore string parsing errors.

        """
        super().__init__(path)
        self._ulog = core.ULog(
            log_file=path,
            message_name_filter_list=message_name_filter_list,
            disable_str_exceptions=disable_str_exceptions,
            parse_header_only=False,
        )

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the ULog."""
        return {
            **super().metadata,
            "msg_info_dict": self.msg_info_dict,
            "default_parameters": self.default_parameters,
            "initial_parameters": self.initial_parameters,
            "changed_parameters": self.changed_parameters,
            "message_formats": self.message_formats,
            "dropouts": self.dropouts,
            "has_data_appended": self.has_data_appended,
            "file_corruption": self.file_corruption,
            "has_default_parameters": self.has_default_parameters,
        }

    @property
    def total_message_count(self) -> int:
        """Return the total number of messages."""
        return sum(len(d.data) for d in self._ulog.data_list)

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return self._ulog.start_timestamp / SECOND

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return self._ulog.last_timestamp / SECOND

    @property
    def msg_info_dict(self) -> dict[str, Any]:
        """Return the message info dictionary."""
        return self._ulog.msg_info_dict

    @property
    def default_parameters(self) -> dict[str, Any]:
        """Return the default parameters."""
        return {
            "system": self._ulog.get_default_parameters(0),
            "current_setup": self._ulog.get_default_parameters(1),
        }

    @property
    def initial_parameters(self) -> dict[str, Any]:
        """Return the initial parameters."""
        return self._ulog.initial_parameters

    @property
    def changed_parameters(self) -> dict[str, Any]:
        """Return the changed parameters."""
        return self._ulog.changed_parameters

    @property
    def message_formats(self) -> dict[str, list[dict[str, str | int]]]:
        """Return the message formats."""
        return {
            name: [
                {"name": field[2], "type": field[0], "array_size": field[1]}
                for field in msg_format.fields
            ]
            for name, msg_format in self._ulog.message_formats.items()
        }

    @property
    def dropouts(self) -> list[dict[str, int]]:
        """Return the list of dropouts."""
        return [
            {"timestamp": dropout.timestamp, "duration": dropout.duration}
            for dropout in self._ulog.dropouts
        ]

    @property
    def has_data_appended(self) -> bool:
        """Return whether data has been appended to the log."""
        return bool(self._ulog.has_data_appended)

    @property
    def file_corruption(self) -> bool:
        """Return whether the log file is corrupted."""
        return self._ulog.file_corruption

    @property
    def has_default_parameters(self) -> bool:
        """Return whether the log has default parameters."""
        return bool(self._ulog.has_default_parameters)

    def build(self) -> core.ULog:
        """Return a PX4 ULog object."""
        return self._ulog

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the PX4 ULog file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".ulg":
            return False, errors.InvalidFileExtensionError(".ulg", self.path)

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
