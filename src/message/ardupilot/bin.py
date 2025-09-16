"""A message dataset for Ardupilot Dataflash logs."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from pymavlink import DFReader

from src.message import base


class MessageDataset(base.MessageDataset):
    """A message dataset for Ardupilot Dataflash logs."""

    def _messages(
        self,
        data_source: DFReader.DFReader_binary,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_exclusive: float | None,
    ) -> Iterator[str, float, DFReader.DFMessage]:
        """Return an iterator of format name, timestamp in seconds, and DFMessage."""
        data_source.rewind()
        while msg := data_source.recv_match(type=topics):
            if start_seconds_inclusive is not None and msg._timestamp < start_seconds_inclusive:
                continue
            if end_seconds_exclusive is not None and msg._timestamp >= end_seconds_exclusive:
                break
            yield msg.get_type(), msg._timestamp, msg

    def _to_json(self, message: DFReader.DFMessage, struct: pa.StructType) -> dict[str, Any]:
        """Cast a DFMessage into a JSON-serializable dictionary."""
        return message.to_dict()
