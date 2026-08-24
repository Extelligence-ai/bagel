"""A message dataset for MCAP files, independent of any robotics middleware.

Messages are decoded per channel encoding: ros1/ros2/protobuf via the registered
decoder factories (which parse the schema embedded in the file), and json-encoded
channels via plain ``json.loads`` -- no ROS installation needed for any of them.
"""

import functools
import json
from collections.abc import Callable, Iterator
from typing import Any

import pyarrow as pa
from mcap.reader import make_reader
from mcap.records import Channel, Schema
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory

from src.di import module
from src.message import base
from src.message.ros2 import convert
from src.source.mcap import McapBag
from src.topic.base import UnsupportedEncodingError

NANOSECOND = 1
SECOND = 1_000_000_000 * NANOSECOND

DECODER_FACTORIES = [
    Ros1DecoderFactory(),
    Ros2DecoderFactory(),
    ProtobufDecoderFactory(),
]


def decoder_for(schema: Schema | None, channel: Channel) -> Callable[[bytes], object]:
    """Return a decode function for a channel, by its message encoding.

    Raises:
        UnsupportedEncodingError: If no decoder handles the channel's encoding.

    """
    if channel.message_encoding == "json":
        return json.loads
    for factory in DECODER_FACTORIES:
        decoder = factory.decoder_for(channel.message_encoding, schema)
        if decoder is not None:
            return decoder
    raise UnsupportedEncodingError(
        f"No decoder for message encoding '{channel.message_encoding}' on topic '{channel.topic}'"
    )


class MessageDataset(base.MessageDataset):
    """A message dataset for MCAP files."""

    def _messages(
        self,
        data_source: McapBag,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, object]]:
        """Return an iterator of topic name, timestamp in seconds, and decoded message."""
        for mcap_file in data_source.mcap_files:
            with open(mcap_file, "rb") as stream:
                reader = make_reader(stream)
                start_time = (
                    start_seconds_inclusive * SECOND
                    if start_seconds_inclusive is not None
                    else None
                )
                decoders: dict[int, Callable[[bytes], object]] = {}
                messages = reader.iter_messages(
                    topics, start_time=start_time, end_time=None, log_time_order=True
                )
                for schema, channel, message in messages:
                    timestamp_seconds = message.log_time / SECOND
                    if (
                        end_seconds_inclusive is not None
                        and timestamp_seconds > end_seconds_inclusive
                    ):
                        return
                    if channel.id not in decoders:
                        decoders[channel.id] = decoder_for(schema, channel)
                    yield channel.topic, timestamp_seconds, decoders[channel.id](message.data)

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Cast a decoded message into a JSON-serializable dictionary."""
        if isinstance(message, dict):
            # json-encoded channels decode to dictionaries already; only walk
            # them when the schema has newtype-shaped fields (see below).
            if _has_newtype_struct(struct):
                wrapped = _wrap_newtypes(message, struct)
                return wrapped if isinstance(wrapped, dict) else message
            return message
        return convert.to_json(message, struct)


def _is_newtype_struct(data_type: pa.DataType) -> bool:
    """Return whether ``data_type`` is a single-field struct.

    That is how serde-transparent newtypes (e.g. unit wrappers) appear in a
    jsonschema traced from the Rust type, while the JSON carries the bare scalar.
    """
    return pa.types.is_struct(data_type) and data_type.num_fields == 1


@functools.lru_cache(maxsize=256)
def _has_newtype_struct(data_type: pa.DataType) -> bool:
    if pa.types.is_struct(data_type):
        return _is_newtype_struct(data_type) or any(
            _has_newtype_struct(data_type.field(i).type) for i in range(data_type.num_fields)
        )
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return _has_newtype_struct(data_type.value_type)
    return False


def _wrap_newtypes(value: object, data_type: pa.DataType) -> object:
    """Return ``value`` with bare scalars wrapped into single-field structs.

    Applied recursively wherever ``data_type`` expects a newtype struct; values
    that already match the schema pass through untouched.
    """
    if pa.types.is_struct(data_type):
        if not isinstance(value, dict):
            if value is None or not _is_newtype_struct(data_type):
                return value
            return {data_type.field(0).name: _wrap_newtypes(value, data_type.field(0).type)}
        return {
            key: (
                _wrap_newtypes(item, data_type.field(key).type)
                if data_type.get_field_index(key) != -1
                else item
            )
            for key, item in value.items()
        }
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)) and isinstance(
        value, list
    ):
        return [_wrap_newtypes(item, data_type.value_type) for item in value]
    return value


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
