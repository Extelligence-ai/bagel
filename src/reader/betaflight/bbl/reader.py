"""Base class for Betaflight .bbl file log readers."""

import logging
import pathlib
from collections import Counter
from collections.abc import Iterator
from typing import Any

from orangebox import Parser
from orangebox.types import Frame, FrameType

from src.reader import reader


class MissingMainFramesError(Exception):
    """Raised when no main frames are found in the log."""


TIME_SERIES_FRAME_TYPES = {"I", "P", "G"}


class BblReader(reader.Reader):
    """Base class for Betaflight .bbl file log readers."""

    def __init__(
        self,
        robolog_path: str | pathlib.Path,
        log_index: int,
        use_cache: bool = True,
    ) -> None:
        """Initialize the BblReader."""
        super().__init__(robolog_path, use_cache, log_index=log_index)

        self._log_index = log_index

        parser = Parser.load(str(robolog_path), log_index=self._log_index)

        self._metadata = parser.headers

        first_main_frame = None
        last_main_frame = None
        self._topic_counts = Counter()
        for frame in parser.frames():
            if frame.type.value not in TIME_SERIES_FRAME_TYPES:
                continue
            if frame.type in {FrameType.INTER, FrameType.INTRA}:  # I or P frames
                if first_main_frame is None:
                    first_main_frame = frame
                last_main_frame = frame
            self._topic_counts[frame.type.value] += 1

        if first_main_frame is None or last_main_frame is None:
            raise MissingMainFramesError(robolog_path)

        self._start_seconds = first_main_frame.data[1] / 1_000_000
        self._end_seconds = last_main_frame.data[1] / 1_000_000

    @property
    def metadata(self) -> dict[str, Any]:
        """Return robolog metadata as a JSON-serializable dictionary."""
        return self._metadata

    @property
    def start_seconds(self) -> float:
        """Return robolog start time in seconds."""
        return self._start_seconds

    @property
    def end_seconds(self) -> float:
        """Return robolog end time in seconds."""
        return self._end_seconds

    @property
    def size_bytes(self) -> int:
        """Return robolog size in bytes."""
        return self.path.stat().st_size

    @property
    def topics(self) -> list[str]:
        """Return a list of topics in the robolog."""
        return list(self._topic_counts.keys())

    @property
    def type_names(self) -> dict[str, str]:
        """Return a mapping of topic names to their message type names."""
        return {topic: topic for topic in self.topics}

    @property
    def message_counts(self) -> dict[str, int]:
        """Return a mapping of topic names to their message counts."""
        return dict(self._topic_counts)

    @property
    def logging_messages(self) -> Iterator[reader.LoggingMessage]:
        """Iterate over logging messages in the robolog."""
        logging.warning("Logging messages are not implemented for BBL logs.")
        return

    def _iter_messages(
        self,
        topics: list[str],
        start_seconds: float | None,
        end_seconds: float | None,
        timestamps_only: bool,
    ) -> Iterator[tuple[float, str, Frame | None]]:
        """Iterate over messages for the specified topics and time range.

        Args:
            topics (list[str]): A list of topic names to read messages from.
            start_seconds (float | None): When to start reading messages.
            end_seconds (float | None): When to stop reading messages.
            timestamps_only (bool): If True, only return timestamps and topic names without message.

        Yields:
            Iterator[tuple[float, str, Frame | None]]: A generator that yields tuples
                containing the timestamp in seconds, topic name, and message (or None if
                `timestamps_only` is True).

        """
        parser = Parser.load(str(self.path), log_index=self._log_index)

        for frame in parser.frames():
            if frame.type.value not in topics:
                continue

            match frame.type:
                case FrameType.INTRA | FrameType.INTER:
                    timestamp_seconds = frame.data[1] / 1_000_000
                case FrameType.GPS:
                    timestamp_seconds = frame.data[0] / 1_000_000
                case _:
                    continue

            if start_seconds is not None and timestamp_seconds < start_seconds:
                continue
            if end_seconds is not None and timestamp_seconds > end_seconds:
                break

            yield timestamp_seconds, frame.type.value, None if timestamps_only else frame
