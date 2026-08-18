"""A message dataset for PyArrow dataset."""

import heapq
from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from src.message import base
from src.source import errors
from src.source.pyarrow.base import PyArrowDataset
from src.topic.pyarrow.base import TOPIC_NAME


class MessageDataset(base.MessageDataset):
    """A message dataset for PyArrow dataset."""

    def _messages(
        self,
        data_source: PyArrowDataset,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        if topics != [TOPIC_NAME]:
            raise ValueError(f"Only '{TOPIC_NAME}' topic is supported for PyArrow data source.")

        heap = []

        # dataset construction (_build(), src/source/pyarrow/base.py) can succeed
        # while still holding an unreadable file: e.g. a directory whose schema is
        # taken from one valid file, alongside a malformed sibling that only fails
        # once actually scanned. to_table() is where that scan -- and thus the raw
        # pyarrow.lib.ArrowInvalid/ArrowTypeError (ArrowException) -- happens.
        try:
            rows = data_source.dataset.to_table().to_pylist()
        except pa.ArrowException as exc:
            files = getattr(data_source.dataset, "files", None)
            location = ", ".join(files) if files else "the PyArrow dataset"
            raise errors.InvalidPathError(
                f"{location} could not be parsed: {type(exc).__name__}: {exc}"
            ) from exc

        for tie_breaker, msg in enumerate(rows):
            timestamp_seconds = data_source.extract_timestamp_seconds(msg)
            if end_seconds_inclusive is not None and timestamp_seconds > end_seconds_inclusive:
                continue
            if start_seconds_inclusive is not None and timestamp_seconds < start_seconds_inclusive:
                continue
            heapq.heappush(heap, (timestamp_seconds, tie_breaker, msg))

        while heap:
            timestamp_seconds, _, msg = heapq.heappop(heap)
            yield (TOPIC_NAME, timestamp_seconds, msg)

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        return message
