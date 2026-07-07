"""A topic registry for MCAP files, independent of any robotics middleware.

Topic names, type names, and message counts come from the MCAP summary (channels,
schemas, statistics). Schemas are converted to PyArrow structs by their embedded
encoding: ``ros2msg`` via the ros2msg grammar, ``protobuf`` via the embedded file
descriptor set, and ``jsonschema`` via a JSON-Schema type mapper. Other encodings
can be listed and described but not queried yet.
"""

import json

import pyarrow as pa
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool

from src.di import module
from src.source.mcap import McapBag, summaries
from src.topic import base
from src.topic.ros2.protobuf import schema as protobuf_schema
from src.topic.ros2.ros2msg import parse as ros2msg_parse
from src.topic.ros2.ros2msg import schema as ros2msg_schema


def _channels_with_schemas(data_source: McapBag) -> dict[str, base.MessageDefinition | None]:
    """Return a mapping from topic name to its message definition (None if schema-less)."""
    topics: dict[str, base.MessageDefinition | None] = {}
    for summary in summaries(data_source):
        for channel in summary.channels.values():
            schema = summary.schemas.get(channel.schema_id)
            if schema is not None:
                topics[channel.topic] = base.MessageDefinition(
                    encoding=schema.encoding, definition=schema.data
                )
            else:
                topics.setdefault(channel.topic, None)
    return topics


def _jsonschema_type(schema: dict) -> pa.DataType:
    """Map a JSON-Schema type definition to a PyArrow type (utf8 for unknowns)."""
    json_type = schema.get("type")
    if isinstance(json_type, list):  # e.g. ["number", "null"] -> nullable number
        json_type = next((t for t in json_type if t != "null"), "string")

    match json_type:
        case "object":
            properties = schema.get("properties", {})
            if not properties:
                return pa.string()  # schemaless object: keep as JSON text
            return pa.struct(
                [pa.field(name, _jsonschema_type(sub)) for name, sub in properties.items()]
            )
        case "number":
            return pa.float64()
        case "integer":
            return pa.int64()
        case "boolean":
            return pa.bool_()
        case "array":
            return pa.list_(_jsonschema_type(schema.get("items", {})))
        case _:
            return pa.string()


def jsonschema_to_struct(definition: bytes) -> pa.StructType:
    """Convert a JSON-Schema document (top-level object) to a PyArrow StructType.

    Raises:
        UnsupportedEncodingError: If the document is not valid JSON or not an object schema.

    """
    try:
        document = json.loads(definition)
    except json.JSONDecodeError as error:
        raise base.UnsupportedEncodingError(f"invalid jsonschema document: {error}") from error
    struct = _jsonschema_type(document if isinstance(document, dict) else {})
    if not isinstance(struct, pa.StructType):
        raise base.UnsupportedEncodingError("jsonschema document is not an object schema")
    return struct


def _type_names(data_source: McapBag) -> dict[str, str]:
    """Return a mapping from topic name to its native type name."""
    names: dict[str, str] = {}
    for summary in summaries(data_source):
        for channel in summary.channels.values():
            schema = summary.schemas.get(channel.schema_id)
            names[channel.topic] = schema.name if schema else channel.message_encoding
    return names


class TopicRegistry(base.TopicRegistry):
    """A topic registry for MCAP files."""

    def available_topics(self, data_source: McapBag) -> list[str]:
        """Return a list of available topic names."""
        return sorted(_type_names(data_source))

    def native_type_name(self, topic: str, data_source: McapBag) -> str:
        """Return the native type name for the given topic."""
        names = _type_names(data_source)
        if topic not in names:
            raise base.TopicNotFoundError(topic)
        return names[topic]

    def message_count(self, topic: str, data_source: McapBag) -> int | None:
        """Return the number of messages for the given topic."""
        found, count = False, 0
        for summary in summaries(data_source):
            channel_ids = [
                channel_id
                for channel_id, channel in summary.channels.items()
                if channel.topic == topic
            ]
            found = found or bool(channel_ids)
            if summary.statistics is None:
                continue
            count += sum(
                summary.statistics.channel_message_counts.get(channel_id, 0)
                for channel_id in channel_ids
            )
        if not found:
            raise base.TopicNotFoundError(topic)
        return count

    def _definition(self, topic: str, data_source: McapBag) -> base.MessageDefinition:
        definitions = _channels_with_schemas(data_source)
        if topic not in definitions:
            raise base.TopicNotFoundError(topic)
        definition = definitions[topic]
        if definition is None:
            raise base.UnsupportedEncodingError(f"topic '{topic}' has no schema")
        return definition

    def struct(self, topic: str, data_source: McapBag) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        definition = self._definition(topic, data_source)
        match definition.encoding:
            case "ros2msg":
                main, deps = ros2msg_parse.parse(definition.definition.decode("utf-8"))
                return ros2msg_schema.to_pa_struct(main, deps)

            case "protobuf":
                pool = DescriptorPool()
                file_descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(
                    definition.definition
                )
                for file_descriptor in file_descriptor_set.file:
                    pool.Add(file_descriptor)
                descriptor = pool.FindMessageTypeByName(
                    self.native_type_name(topic, data_source)
                )
                return protobuf_schema.to_pa_struct(descriptor)

            case "jsonschema":
                return jsonschema_to_struct(definition.definition)

            case _:
                raise base.UnsupportedEncodingError(definition.encoding)

    def describe(self, topic: str, data_source: McapBag) -> str:
        """Return a human-readable description of the given topic."""
        definition = self._definition(topic, data_source)
        match definition.encoding:
            case "ros1msg" | "ros2msg":
                return definition.definition.decode("utf-8")

            case "protobuf":
                return str(descriptor_pb2.FileDescriptorSet.FromString(definition.definition))

            case "jsonschema":
                return json.dumps(json.loads(definition.definition), indent=2)

            case _:
                raise base.UnsupportedEncodingError(definition.encoding)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
