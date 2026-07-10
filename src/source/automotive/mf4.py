"""Provide a data source for reading ASAM MDF (.mf4) measurement files.

MDF is the standard measurement format in automotive (ASAM MDF4, written by CANape,
INCA, vector tooling, and most DAQ hardware). Channel groups map to Bagel topics and
channels to fields. Requires the optional ``automotive`` dependency group:
``uv sync --group automotive``.
"""

import functools
from typing import Any

from asammdf import MDF

from src.di import module
from src.source import base, errors

MDF_MAGIC = b"MDF     "


class SourceFactory(base.BoundedSourceFactory, base.FileBasedSourceFactory):
    """A data source factory for reading ASAM MDF (.mf4) files."""

    def __init__(self, path: str) -> None:
        """Initialize the MDF data source factory.

        Args:
            path (str): Path to the .mf4 (or older .mdf) measurement file.

        """
        super().__init__(path)
        self._mdf = MDF(path)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the measurement file."""
        return {
            **self._bounded_metadata,
            **self._file_based_metadata,
            "mdf_version": self._mdf.version,
            "measurement_start": str(self._mdf.header.start_time),
            "channel_groups": [
                {
                    "name": group.channel_group.acq_name or f"ChannelGroup_{index}",
                    "cycles": group.channel_group.cycles_nr,
                    "channels": [channel.name for channel in group.channels],
                }
                for index, group in enumerate(self._mdf.groups)
            ],
        }

    @property
    def total_message_count(self) -> int:
        """Return the total number of samples across all channel groups."""
        return sum(group.channel_group.cycles_nr for group in self._mdf.groups)

    @functools.cached_property
    def start_seconds(self) -> float:
        """Return the measurement start as epoch seconds."""
        return self._mdf.header.start_time.timestamp()

    @functools.cached_property
    def end_seconds(self) -> float:
        """Return the epoch seconds of the last sample in any channel group."""
        last = 0.0
        for index in range(len(self._mdf.groups)):
            master = self._mdf.get_master(index)
            if len(master):
                last = max(last, float(master[-1]))
        return self.start_seconds + last

    def build(self) -> MDF:
        """Return the MDF reader object."""
        return self._mdf

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate that the path is an ASAM MDF file."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        with open(self.path, "rb") as stream:
            if stream.read(len(MDF_MAGIC)) != MDF_MAGIC:
                return False, errors.InvalidPathError(f"{self.path} is not an ASAM MDF file.")

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
