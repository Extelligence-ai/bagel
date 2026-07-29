"""Provide a data source for raw CAN logs (.blf / .asc) decoded through a DBC.

A DBC file is the schema of a CAN bus: it names messages and scales their signals
to physical values. Bagel maps DBC messages to topics and signals to fields, so a
raw bus capture becomes queryable like any other source. Requires the optional
``automotive`` dependency group: ``uv sync --group automotive``.
"""

import functools
import logging
import pathlib
import struct
from typing import Any

import can
import cantools

from src.di import module
from src.source import base, errors

BLF_MAGIC = b"LOGG"


class CanLog:
    """A DBC-decoded view over a raw CAN log file."""

    def __init__(self, path: str, dbc: str) -> None:
        """Initialize the decoded log.

        Args:
            path (str): Path to the .blf or .asc capture.
            dbc (str): Path to the DBC database describing the bus.

        """
        try:
            self.database = cantools.database.load_file(dbc)
        except (cantools.errors.Error, ValueError) as exc:
            # Empirically, a malformed/garbage/empty DBC raises
            # cantools.database.errors.UnsupportedDatabaseFormatError (a
            # cantools.errors.Error subclass), while a DBC path with an
            # extension cantools can't map to a format raises a plain
            # ValueError from cantools' own format-dispatch code. Translate
            # either into a single clean, typed error instead of letting a
            # cantools-internal exception escape.
            raise errors.InvalidPathError(
                f"{dbc} could not be parsed as a DBC database: {type(exc).__name__}: {exc}"
            ) from exc
        self._path = path
        self._records: list[tuple[float, str, dict[str, Any]]] | None = None

    @property
    def records(self) -> list[tuple[float, str, dict[str, Any]]]:
        """(timestamp, message_name, decoded_signals) tuples, sorted by time."""
        if self._records is None:
            known = {message.frame_id: message.name for message in self.database.messages}
            records = []
            unknown = 0
            undecodable = 0
            try:
                # This region wraps library parse calls over untrusted input
                # (the capture file itself), so failures here are translated
                # into a clean, typed error rather than leaking a can/cantools
                # traceback. It's scoped to STRUCTURAL capture failures only:
                # the file can't be opened, or frame iteration itself breaks
                # down (corrupt/truncated container). A single frame that
                # fails to decode is handled by the inner skip-and-count
                # below and never reaches this except clause.
                with can.LogReader(self._path) as reader:
                    for frame in reader:
                        name = known.get(frame.arbitration_id)
                        if name is None:
                            unknown += 1
                            continue
                        try:
                            decoded = self.database.decode_message(
                                frame.arbitration_id, frame.data, decode_choices=False
                            )
                            records.append((float(frame.timestamp), name, dict(decoded)))
                        except (cantools.database.errors.DecodeError, ValueError) as exc:
                            # A single frame with a data length mismatch (or
                            # other per-frame decode problem) shouldn't nuke
                            # an otherwise-good capture: skip and count it,
                            # mirroring the `unknown` handling above.
                            undecodable += 1
                            logging.debug(
                                "Skipping undecodable frame (arbitration_id=%s): %s: %s",
                                frame.arbitration_id,
                                type(exc).__name__,
                                exc,
                            )
                            continue
            except (struct.error, ValueError, cantools.errors.Error) as exc:
                # Empirically observed across malformed/truncated .blf and
                # .asc captures: struct.error and ValueError escape
                # can.LogReader(...) construction (corrupt/inconsistent BLF
                # headers, unrecognized capture extensions) or frame
                # iteration (non-hex ASC frame data). These share no common
                # base narrower than Exception, so translate either of them
                # into a single clean, typed error instead of letting a
                # can/cantools-internal exception escape.
                raise errors.InvalidPathError(
                    f"{self._path} could not be read as a CAN capture: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if unknown:
                logging.info("Skipped %d frames with IDs not in the DBC", unknown)
            if undecodable:
                logging.info("Skipped %d frames that failed to decode", undecodable)
            records.sort(key=lambda record: record[0])
            self._records = records
        return self._records


class SourceFactory(base.BoundedSourceFactory, base.FileBasedSourceFactory):
    """A data source factory for DBC-decoded raw CAN logs."""

    def __init__(self, path: str, dbc: str) -> None:
        """Initialize the CAN log data source factory.

        Args:
            path (str): Path to the .blf or .asc capture.
            dbc (str): Path to the DBC database describing the bus (pass via the
                tool's `args`, e.g. ``args={"dbc": "./vehicle.dbc"}``).

        """
        self._dbc = dbc
        super().__init__(path)
        self._log = CanLog(path=path, dbc=dbc)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the capture and its DBC."""
        return {
            **self._bounded_metadata,
            **self._file_based_metadata,
            "dbc": self._dbc,
            "dbc_messages": [message.name for message in self._log.database.messages],
        }

    @property
    def total_message_count(self) -> int:
        """Return the number of decodable frames."""
        return len(self._log.records)

    @functools.cached_property
    def start_seconds(self) -> float:
        """Return the first frame's timestamp in seconds."""
        records = self._log.records
        return records[0][0] if records else 0.0

    @functools.cached_property
    def end_seconds(self) -> float:
        """Return the last frame's timestamp in seconds."""
        records = self._log.records
        return records[-1][0] if records else 0.0

    def build(self) -> CanLog:
        """Return the decoded log."""
        return self._log

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate that the path is a BLF or ASC capture and the DBC exists."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not pathlib.Path(self._dbc).is_file():
            return False, FileNotFoundError(f"DBC database not found: {self._dbc}")

        with open(self.path, "rb") as stream:
            is_blf = stream.read(len(BLF_MAGIC)) == BLF_MAGIC
        if not is_blf and self.path.suffix.lower() != ".asc":
            return False, errors.InvalidPathError(f"{self.path} is not a BLF or ASC CAN capture.")

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
