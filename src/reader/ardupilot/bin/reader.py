"""Base class for ArduPilot Dataflash log readers."""

import pathlib
from collections.abc import Iterator
from typing import Any

from pymavlink import DFReader

from src.reader import reader


class BinReader(reader.Reader):
    """Base class for ArduPilot Dataflash log readers."""

    def __init__(
        self,
        robolog_path: str | pathlib.Path,
        use_cache: bool = True,
        zero_time_base: bool = False,
    ) -> None:
        """Initialize the BinReader."""
        super().__init__(robolog_path, use_cache)
        self._df_reader = DFReader.DFReader_binary(
            filename=str(robolog_path), zero_time_base=zero_time_base
        )
        self._metadata = self._df_reader.metadata

        # gather start and end seconds
        self._df_reader.rewind()
        msg = self._df_reader.recv_msg()
        self._start_seconds = msg._timestamp
        self._end_seconds = self._df_reader.last_timestamp()
        self._df_reader.rewind()

    @property
    def metadata(self) -> dict[str, Any]:
        """Return robolog metadata as a JSON-serializable dictionary."""
        params = {}
        for _, _, msg in self._iter_messages(["PARM"], None, None, False):
            msg_dict = msg.to_dict()
            params[msg_dict["Name"]] = msg_dict["Value"]
        return params

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
        return list(self.message_counts.keys())

    @property
    def type_names(self) -> dict[str, str]:
        """Return a mapping of topic names to their message type names."""
        return {topic: topic for topic in self.topics}

    @property
    def message_counts(self) -> dict[str, int]:
        """Return a mapping of topic names to their message counts."""
        counts = {}
        for type_id, count in enumerate(self._df_reader.counts):
            if count > 0 and type_id in self._df_reader.id_to_name:
                type_name = self._df_reader.id_to_name[type_id]
                counts[type_name] = count
        return counts

    @property
    def logging_messages(self) -> Iterator[reader.LoggingMessage]:
        """Iterate over logging messages in the robolog."""
        for timestamp_seconds, _, msg in self._iter_messages(["MSG"], None, None, False):
            yield reader.LoggingMessage(
                robolog_id=self.robolog_id,
                timestamp_seconds=timestamp_seconds,
                level="MSG",  # no formal level in the MSG type
                message=msg.to_dict()["Message"],
            )

    def _iter_messages(
        self,
        topics: list[str],
        start_seconds: float | None,
        end_seconds: float | None,
        timestamps_only: bool,
    ) -> Iterator[tuple[float, str, DFReader.DFMessage | None]]:
        """Iterate over messages for the specified topics and time range.

        Args:
            topics (list[str]): A list of topic names to read messages from.
            start_seconds (float | None): When to start reading messages.
            end_seconds (float | None): When to stop reading messages.
            timestamps_only (bool): If True, only return timestamps and topic names without message.

        Yields:
            Iterator[tuple[float, str, DFReader.DFMessage | None]]: A generator that yields tuples
                containing the timestamp in seconds, topic name, and message (or None if
                `timestamps_only` is True).

        """
        self._df_reader.rewind()
        while msg := self._df_reader.recv_match(type=topics):
            if start_seconds is not None and msg._timestamp <= start_seconds:
                continue
            if end_seconds is not None and msg._timestamp > end_seconds:
                break
            yield msg._timestamp, msg.get_type(), None if timestamps_only else msg
