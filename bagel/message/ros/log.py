"""A message dataset for plain-text ROS log files."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from bagel.di import module
from bagel.message import base
from bagel.source.ros.log import RosLogSource


class MessageDataset(base.MessageDataset):
    """A message dataset for plain-text ROS log files.

    Topics are node names; each message is a {level, message, file} struct.
    """

    def _messages(
        self,
        data_source: RosLogSource,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        wanted = set(topics)
        for record in data_source.records:
            if record.node not in wanted:
                continue
            if (
                start_seconds_inclusive is not None
                and record.timestamp_seconds < start_seconds_inclusive
            ):
                continue
            if (
                end_seconds_inclusive is not None
                and record.timestamp_seconds > end_seconds_inclusive
            ):
                continue
            yield (
                record.node,
                record.timestamp_seconds,
                {"level": record.level, "message": record.message, "file": record.file},
            )

    def _to_json(self, message: dict[str, Any], struct: pa.StructType) -> dict[str, Any]:
        return message


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
