"""A message dataset for DBC-decoded CAN logs."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from src.di import module
from src.message import base
from src.source.automotive.can import CanLog


class MessageDataset(base.MessageDataset):
    """A message dataset for DBC-decoded CAN logs.

    Topics are DBC messages; each message is one decoded frame with a field per
    signal (physical values, scaling applied by the DBC).
    """

    def _messages(
        self,
        data_source: CanLog,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        wanted = set(topics)
        for timestamp, name, decoded in data_source.iter_records(
            start_seconds_inclusive, end_seconds_inclusive
        ):
            if name not in wanted:
                continue
            yield (name, timestamp, {key: float(value) for key, value in decoded.items()})

    def _to_json(self, message: dict[str, Any], struct: pa.StructType) -> dict[str, Any]:
        return message


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
