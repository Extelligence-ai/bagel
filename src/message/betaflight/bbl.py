"""A message dataset for Betaflight Blackbox logs."""

from collections.abc import Iterator
from typing import Any

import orangebox
import pyarrow as pa
from orangebox.types import Frame, FrameType

from src import di
from src.message import base

MICROSECOND = 1
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class UnsupportedFrameTypeError(Exception):
    """Raised when an unsupported frame type is encountered."""


class MessageDataset(base.MessageDataset):
    """A message dataset for Betaflight Blackbox logs."""

    def _messages(
        self,
        data_source: orangebox.Parser,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_exclusive: float | None,
    ) -> Iterator[str, float, Frame]:
        """Return an iterator of frame type, timestamp in seconds, and a Frame."""
        for frame in data_source.frames():
            if frame.type.value not in topics:
                continue

            match frame.type:
                case FrameType.INTRA | FrameType.INTER:
                    timestamp_seconds = frame.data[1] / SECOND  # "time" field
                case FrameType.GPS:
                    timestamp_seconds = frame.data[0] / SECOND  # "time" field
                case _:
                    raise UnsupportedFrameTypeError(frame.type.name)

            if start_seconds_inclusive is not None and timestamp_seconds < start_seconds_inclusive:
                continue
            if end_seconds_exclusive is not None and timestamp_seconds >= end_seconds_exclusive:
                continue

            yield frame.type.value, timestamp_seconds, frame

    def _to_json(self, message: Frame, struct: pa.StructType) -> dict[str, Any]:
        """Cast a Frame into a JSON-serializable dictionary."""
        return {name: value for name, value in zip(struct.names, message.data, strict=False)}


def register() -> None:
    """Register module for dependency injection."""
    di.module_registry[__name__] = MessageDataset
