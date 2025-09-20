"""A message dataset for ROS2 MCAP bags."""

from collections.abc import Iterator
from typing import Any

import pyarrow as pa
from mcap.reader import make_reader
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory

from src.di import module
from src.message import base
from src.message.ros2 import convert
from src.source.ros2.mcap import McapRos2Bag

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND

DECODER_FACTORIES = [
    Ros1DecoderFactory(),
    Ros2DecoderFactory(),
    ProtobufDecoderFactory(),
]


class MessageDataset(base.MessageDataset):
    """A message dataset for ROS2 MCAP bags."""

    def _messages(
        self,
        data_source: McapRos2Bag,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_exclusive: float | None,
    ) -> Iterator[str, float, object]:
        """Return an iterator of topic name, timestamp in seconds, and deserialized ROS2 message."""
        for mcap_file in data_source.mcap_files:
            with open(mcap_file, "rb") as stream:
                reader = make_reader(stream, decoder_factories=DECODER_FACTORIES)
                messages = reader.iter_decoded_messages(
                    topics,
                    start_seconds_inclusive * SECOND if start_seconds_inclusive else None,
                    end_seconds_exclusive * SECOND if end_seconds_exclusive else None,
                )
                for _, channel, message, decoded_message in messages:
                    yield channel.topic, message.log_time / SECOND, decoded_message

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Cast a deserialized ROS2 message into a JSON-serializable dictionary."""
        return convert.to_json(message, struct)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
