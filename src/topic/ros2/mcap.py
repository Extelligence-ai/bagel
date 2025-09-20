"""A topic registry for ROS2 MCAP bags."""

import functools

import pyarrow as pa
import rosbag2_py
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from mcap.reader import make_reader

from src.di import module
from src.source.ros2.mcap import McapRos2Bag
from src.topic.ros2 import base
from src.topic.ros2.protobuf import schema as protobuf_schema
from src.topic.ros2.ros2msg import parse as ros2msg_parse
from src.topic.ros2.ros2msg import schema as ros2msg_schema


@functools.lru_cache
def message_definitions(data_source: McapRos2Bag) -> dict[str, base.MessageDefinition]:
    """Return a mapping from message type name to its definition and encoding."""
    schemas = []
    for file in data_source.mcap_files:
        with open(file, "rb") as stream:
            summary = make_reader(stream).get_summary()
            schemas.extend(list(summary.schemas.values()))
    return {s.name: base.MessageDefinition(encoding=s.encoding, definition=s.data) for s in schemas}


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ROS2 MCAP bags."""

    def struct(self, topic: str, data_source: McapRos2Bag) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        type_name = self.native_type_name(topic, data_source)
        definition = message_definitions(data_source)[type_name]
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
                descriptor = pool.FindMessageTypeByName(type_name)
                return protobuf_schema.to_pa_struct(descriptor)

            case _:
                raise base.UnsupportedEncodingError(definition.encoding)

    def describe(self, topic: str, data_source: McapRos2Bag) -> str:
        """Return a human-readable description of the given topic."""
        type_name = self.native_type_name(topic, data_source)
        definition = message_definitions(data_source)[type_name]
        match definition.encoding:
            case "ros1msg" | "ros2msg":
                return definition.definition.decode("utf-8")

            case "protobuf":
                return str(descriptor_pb2.FileDescriptorSet.FromString(definition.definition))

            case _:
                raise base.UnsupportedEncodingError(definition.encoding)

    def _metadata(self, data_source: McapRos2Bag) -> rosbag2_py.BagMetadata:
        return data_source.metadata


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
