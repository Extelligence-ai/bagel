"""A topic registry for ROS2 bags."""

import collections
import functools
import pathlib

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
def locally_installed_ros2msg(type_name: str) -> str:
    """Return the ros2msg string of the given message type from locally installed packages.

    If packages are not installed or sourced locally, this will throw an error.

    """
    from rosidl_parser.definition import NamespacedType
    from rosidl_runtime_py import get_interface_path
    from rosidl_runtime_py.utilities import get_message

    def resolve(name: str) -> str:
        match tuple(name.split("/")):
            case (package, "msg", class_):
                return name
            case (package, class_):
                return f"{package}/msg/{class_}"
            case _:
                raise ValueError(f"Invalid type name: {name}")

    visited = set()
    dependencies = []
    stack = collections.deque([resolve(type_name)])
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        dependencies.append(current)
        for slot_type in get_message(current).SLOT_TYPES:
            if isinstance(slot_type, NamespacedType):
                stack.append("/".join(slot_type.namespaced_name()))

    sections = []
    for dependency_type_name in dependencies:
        msg_file = get_interface_path(dependency_type_name)
        section = pathlib.Path(msg_file).read_text(encoding="utf-8")
        if sections:
            section = f"MSG: {dependency_type_name}\n{section}"
        sections.append(section)

    separator = "=" * 80
    return f"\n{separator}\n".join(sections)


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
            topic_metadata.type: MessageDefinition(
                encoding="ros2msg",
                definition=locally_installed_ros2msg(topic_metadata.type),
            )
            for topic_metadata in data_source.get_all_topics_and_types()
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
