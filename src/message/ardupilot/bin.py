"""A message dataset for Ardupilot Dataflash logs."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from pymavlink import DFReader

from src.di import module
from src.message import base


class MessageDataset(base.MessageDataset):
    """A message dataset for Ardupilot Dataflash logs."""

    def _messages(
        self,
        data_source: DFReader.DFReader_binary,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, DFReader.DFMessage]]:
        """Return an iterator of format name, timestamp in seconds, and DFMessage."""
        data_source.rewind()
        while msg := data_source.recv_match(type=topics):
            if start_seconds_inclusive is not None and msg._timestamp < start_seconds_inclusive:
                continue
            if end_seconds_inclusive is not None and msg._timestamp > end_seconds_inclusive:
                return
            yield msg.get_type(), msg._timestamp, msg

    def _to_json(self, message: DFReader.DFMessage, struct: pa.StructType) -> dict[str, Any]:
        """Cast a DFMessage into a JSON-serializable dictionary.

        ``char[]`` fields (format ``Z``/``n``/``N``) are strings in the schema;
        pymavlink hands back ``bytes`` for values it could not decode, so decode
        them here (replacement characters, NULs stripped) rather than failing
        the whole batch.
        """
        return {key: _text(value) for key, value in message.to_dict().items()}


def _text(value: object) -> object:
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace").rstrip("\x00")
    return value


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
