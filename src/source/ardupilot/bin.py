"""Provide a data source for reading ArduPilot Dataflash logs."""

from typing import Any

from pymavlink import DFReader

from src.di import module
from src.source import base, errors


class SourceFactory(base.FileBasedSourceFactory):
    """A data source factory for reading from ArduPilot Dataflash logs."""

    def __init__(
        self,
        path: str,
        zero_time_base: bool = False,
    ) -> None:
        """Initialize the ArduPilot Dataflash data source factory.

        Args:
            path (str): Path to the .bin file.
            zero_time_base (bool, optional): If True, timestamps start from zero instead of using
                GPS time. False does not guarantee epoch time if no valid GPS messages.

        """
        super().__init__(path)
        self._zero_time_base = zero_time_base

        reader = DFReader.DFReader_binary(filename=path, zero_time_base=zero_time_base)

        # gather message count, start and end seconds
        reader.rewind()
        self._start_seconds = reader.recv_msg()._timestamp
        self._end_seconds = reader.last_timestamp()
        self._total_message_count = sum(reader.counts)

        # gather PARM messages
        reader.rewind()
        self._params = {}
        while msg := reader.recv_match(type=["PARM"]):
            d = msg.to_dict()
            self._params[d["Name"]] = d["Value"]

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the Dataflash log."""
        return {
            **super().metadata,
            **self._params,
        }

    @property
    def total_message_count(self) -> int:
        """Return the total number of messages."""
        return self._total_message_count

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return self._start_seconds

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return self._end_seconds

    def build(self) -> DFReader.DFReader_binary:
        """Return an ArduPilot DFReader_binary object."""
        return DFReader.DFReader_binary(
            filename=str(self.path),
            zero_time_base=self._zero_time_base,
            progress_callback=None,
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ArduPilot Dataflash file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".bin":
            return False, errors.InvalidFileExtensionError(".bin", self.path)

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
