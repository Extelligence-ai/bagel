"""Provide a data source for reading Betaflight Blackbox logs."""

from collections.abc import Iterator
from typing import Any

import orangebox
from orangebox.types import Frame, FrameType

from src.source import base, errors

MICROSECOND = 1
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from Betaflight Blackbox logs."""

    def __init__(
        self,
        path: str,
        log_index: int,
    ) -> None:
        """Initialize the Betaflight Blackbox data source factory.

        Args:
            path (str): Path to the .bbl file.
            log_index (int): Index within log file. When using a built-in flash chip for logging,
                flight logs are combined into a single .bbl file. The log_index parameter specifies
                which flight log to read from the combined file.

        """
        super().__init__(path)
        self._log_index = log_index

        parser = orangebox.Parser.load(path=path, log_index=log_index, allow_invalid_header=False)
        self._headers = parser.headers
        self._total_message_count, self._end_seconds = self._total_message_count_and_end_seconds(
            parser.frames()
        )

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the Blackbox log."""
        return {
            **super().metadata,
            **self._headers,
        }

    @property
    def total_message_count(self) -> int:
        """Return the total number of messages."""
        return self._total_message_count

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return 0.0  # on Cleanflight it represents the system uptime

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return self._end_seconds

    def build(self) -> orangebox.Parser:
        """Return an orangebox Parser object."""
        return orangebox.Parser.load(
            path=str(self.path), log_index=self._log_index, allow_invalid_header=False
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the Betaflight Blackbox file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".bbl":
            return False, errors.InvalidFileExtensionError(".bbl", self.path)

        return True, None

    def _total_message_count_and_end_seconds(self, frames: Iterator[Frame]) -> tuple[int, float]:
        count, end_time = 0, 0.0
        for frame in frames:
            count += 1
            match frame.type:
                case FrameType.INTRA | FrameType.INTER:
                    end_time = frame.data[1] / SECOND  # "time" field
                case FrameType.GPS:
                    end_time = frame.data[0] / SECOND  # "time" field
                case _:
                    continue
        return count, end_time
