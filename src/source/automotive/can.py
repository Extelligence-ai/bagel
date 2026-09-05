"""Provide a data source for raw CAN logs (.blf / .asc) decoded through a DBC.

A DBC file is the schema of a CAN bus: it names messages and scales their signals
to physical values. Bagel maps DBC messages to topics and signals to fields, so a
raw bus capture becomes queryable like any other source. Requires the optional
``automotive`` dependency group: ``uv sync --group automotive``.
"""

import functools
import heapq
import logging
import math
import pathlib
import struct
from collections.abc import Iterator
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

    def _decoded_frames(self) -> Iterator[tuple[float, str, dict[str, Any]]]:
        """Stream (timestamp, message_name, decoded_signals) without storing them."""
        known = {message.frame_id: message.name for message in self.database.messages}
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
                    yield (float(frame.timestamp), name, dict(decoded))
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
                f"{self._path} could not be read as a CAN capture: {type(exc).__name__}: {exc}"
            ) from exc
        if unknown:
            logging.info("Skipped %d frames with IDs not in the DBC", unknown)
        if undecodable:
            logging.info("Skipped %d frames that failed to decode", undecodable)

    @functools.cached_property
    def _frame_stats(self) -> tuple[int, float, float, dict[str, int]]:
        """One decode pass: (count, first timestamp, last timestamp, per-topic counts).

        Backs both `stats` and `topic_message_counts` so enumerating N topics
        costs one decode pass total, not N (#134). Memory for the per-topic
        dict is bounded by the number of distinct DBC message names, not file
        size, so it stays within the streaming design.
        """
        count = 0
        start = math.inf
        end = -math.inf
        topic_counts: dict[str, int] = {}
        for timestamp, name, _ in self._decoded_frames():
            count += 1
            start = min(start, timestamp)
            end = max(end, timestamp)
            topic_counts[name] = topic_counts.get(name, 0) + 1
        if not count:
            return (0, 0.0, 0.0, {})
        return (count, start, end, topic_counts)

    @functools.cached_property
    def stats(self) -> tuple[int, float, float]:
        """(decodable frame count, first timestamp, last timestamp), one pass, O(1) memory.

        Decode is still attempted per frame so the count matches records()
        exactly; the decoded values are discarded immediately (#134).
        """
        count, start, end, _ = self._frame_stats
        return (count, start, end)

    @functools.cached_property
    def topic_message_counts(self) -> dict[str, int]:
        """Decoded frame count per DBC message name, from the same pass as `stats` (#134)."""
        return self._frame_stats[3]

    def iter_records(
        self,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
        reorder_buffer: int = 10_000,
    ) -> Iterator[tuple[float, str, dict[str, Any]]]:
        """Stream windowed records in timestamp order with bounded memory.

        CAN captures are written (near-)chronologically; a bounded min-heap
        absorbs local interleave across channels. A frame more than
        ``reorder_buffer`` positions out of order would be yielded late, but
        captures that disordered are not produced by known tooling. Peak
        memory is the heap, not the capture (Codex review on #156).
        """
        heap: list[tuple[float, int, tuple[float, str, dict[str, Any]]]] = []
        counter = 0
        for record in self._decoded_frames():
            timestamp = record[0]
            if start_seconds is not None and timestamp < start_seconds:
                continue
            if end_seconds is not None and timestamp > end_seconds:
                continue
            heapq.heappush(heap, (timestamp, counter, record))
            counter += 1
            if len(heap) > reorder_buffer:
                yield heapq.heappop(heap)[2]
        while heap:
            yield heapq.heappop(heap)[2]

    def records(
        self, start_seconds: float | None = None, end_seconds: float | None = None
    ) -> list[tuple[float, str, dict[str, Any]]]:
        """(timestamp, message_name, decoded_signals) in the window, sorted by time.

        Decoded per call and NOT cached on the object: repeated-query
        performance comes from the on-disk Arrow cache in to_duckdb (#134).
        """
        records = [
            record
            for record in self._decoded_frames()
            if (start_seconds is None or record[0] >= start_seconds)
            and (end_seconds is None or record[0] <= end_seconds)
        ]
        records.sort(key=lambda record: record[0])
        return records


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
        self._dbc_digest = self._md5_hash(pathlib.Path(dbc))

    def cache_identity_for(self, source_uuid: str) -> str:
        """Include the loaded DBC's scaling and decoding rules in cached results.

        Overrides `cache_identity_for` (not just the `cache_identity` property) so
        the DBC digest is included regardless of which one a caller uses: `to_duckdb`
        calls this method directly with an already-computed uuid to avoid hashing a
        large capture twice (#232), which would otherwise bypass a `cache_identity`-only
        override entirely.
        """
        return super().cache_identity_for(source_uuid) + self._dbc_digest

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
    def identity_metadata(self) -> dict[str, Any]:
        """Return interpretation options for the cache key without decoding the capture.

        Excludes `_bounded_metadata` (message count, start/end/duration): those are
        derived by fully decoding the capture, and describe content already
        fingerprinted by `uuid` -- a fixed byte-for-byte capture always decodes to the
        same stats, so they add no cache-invalidation signal and would otherwise force
        a full decode before even checking the on-disk cache.
        """
        return {
            **self._file_based_metadata,
            "dbc": self._dbc,
            "dbc_messages": [message.name for message in self._log.database.messages],
        }

    @property
    def total_message_count(self) -> int:
        """Return the number of decodable frames."""
        return self._log.stats[0]

    @functools.cached_property
    def start_seconds(self) -> float:
        """Return the first frame's timestamp in seconds."""
        return self._log.stats[1]

    @functools.cached_property
    def end_seconds(self) -> float:
        """Return the last frame's timestamp in seconds."""
        return self._log.stats[2]

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
