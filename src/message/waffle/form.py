"""A message dataset for WaffleForm files. EXPERIMENTAL BETA."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from src.di import module
from src.message import base
from src.source.waffle.form import WaffleForm


class MessageDataset(base.MessageDataset):
    """A message dataset for WaffleForm files.

    Topics are component categories; each message is one declared component. All
    rows carry the form's snap time, so a WaffleForm behaves as a one-instant
    snapshot -- accumulate snapshots to get hardware state over time.
    """

    def _messages(
        self,
        data_source: WaffleForm,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        timestamp = data_source.snap_seconds
        if start_seconds_inclusive is not None and timestamp < start_seconds_inclusive:
            return
        if end_seconds_inclusive is not None and timestamp > end_seconds_inclusive:
            return
        for topic in topics:
            for row in data_source.rows.get(topic, []):
                yield (topic, timestamp, row)

    def _to_json(self, message: dict[str, Any], struct: pa.StructType) -> dict[str, Any]:
        return message


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
