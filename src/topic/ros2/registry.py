"""A topic registry for ROS2 bags."""

import functools

import pyarrow as pa
import rosbag2_py
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from mcap.reader import make_reader
from pydantic import BaseModel

from src.source.ros2.mcap import McapRos2Bag
from src.topic import base


class UnsupportedEncodingError(Exception):
    """Raised when the message definition's encoding is not supported."""


class MessageDefinition(BaseModel):
    """Contain message definition and its encoding."""

    encoding: str
    definition: bytes


@functools.lru_cache
def message_definitions(
    data_source: McapRos2Bag | rosbag2_py.SequentialReader,
) -> dict[str, MessageDefinition]:
    """Return a mapping from message type name to its definition and encoding."""
    if isinstance(data_source, McapRos2Bag):
        schemas = []
        for file in data_source.mcap_files:
            with open(file, "rb") as stream:
                summary = make_reader(stream).get_summary()
                schemas.extend(list(summary.schemas.values()))
        return {
            s.name: MessageDefinition(
                encoding=s.encoding,
                definition=s.data,
            )
            for s in schemas
        }
    else:
        return {
            d.topic_type: MessageDefinition(
                encoding=d.encoding,
                definition=d.encoded_message_definition,
            )
            for d in data_source.get_all_message_definitions()
        }


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ROS2 bags."""

    def available_topics(self, data_source: McapRos2Bag | rosbag2_py.SequentialReader) -> list[str]:
        """Return a list of available topic names."""
        return sorted(
            [
                info.topic_metadata.name
                for info in self._metadata(data_source).topics_with_message_count
            ]
        )

    def native_type_name(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> str:
        """Return the native type name for the given topic."""
        info = self._topic_info(topic, data_source)
        return info.topic_metadata.type

    def message_count(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> int:
        """Return the number of messages for the given topic."""
        info = self._topic_info(topic, data_source)
        return info.message_count

    def struct(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        type_name = self.native_type_name(topic, data_source)
        definition = message_definitions(data_source)[type_name]

        match definition.encoding:
            case "ros2msg":
                from src.topic.ros2.ros2msg import parse, schema

                main, deps = parse.parse(definition.definition.decode("utf-8"))
                return schema.to_pa_struct(main, deps)

            case "ros1msg":
                from src.topic.ros1 import parse, schema

                main, deps = parse.parse(definition.definition.decode("utf-8"))
                return schema.to_pa_struct(main, deps)

            case "protobuf":
                from src.topic.ros2.protobuf import schema

                pool = DescriptorPool()
                file_descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(
                    definition.definition
                )
                for file_descriptor in file_descriptor_set.file:
                    pool.Add(file_descriptor)
                descriptor = pool.FindMessageTypeByName(type_name)
                return schema.to_pa_struct(descriptor)

            case _:
                raise UnsupportedEncodingError(definition.encoding)

    def describe(self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader) -> str:
        """Return a human-readable description of the given topic."""
        type_name = self.native_type_name(topic, data_source)
        definition = message_definitions(data_source)[type_name]

        match definition.encoding:
            case "ros2msg" | "ros1msg":
                return definition.definition.decode("utf-8")

            case "protobuf":
                return str(descriptor_pb2.FileDescriptorSet.FromString(definition.definition))

            case _:
                raise UnsupportedEncodingError(definition.encoding)

    def _metadata(
        self, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> rosbag2_py.BagMetadata:
        return (
            data_source.metadata
            if isinstance(data_source, McapRos2Bag)
            else data_source.get_metadata()
        )

    def _topic_info(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> rosbag2_py.TopicInformation:
        for info in self._metadata(data_source).topics_with_message_count:
            if info.topic_metadata.name == topic:
                return info
        raise base.TopicNotFoundError(topic)
