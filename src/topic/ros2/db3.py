"""A topic registry for ROS2 sqlite3 bags."""

import collections
import functools
import pathlib

import pyarrow as pa
import rosbag2_py
from rosidl_parser.definition import NamespacedType
from rosidl_runtime_py import get_interface_path
from rosidl_runtime_py.utilities import get_message

from src.di import module
from src.topic.ros2 import base
from src.topic.ros2.ros2msg import parse, schema


@functools.lru_cache
def locally_installed_ros2msg(type_name: str) -> str:
    """Return the ros2msg string of the given message type from locally installed packages.

    If packages are not installed or sourced locally, this will throw an error.

    """

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
    data_source: rosbag2_py.SequentialReader,
) -> dict[str, base.MessageDefinition]:
    """Return a mapping from message type name to its definition and encoding."""
    return {
        topic_metadata.type: base.MessageDefinition(
            encoding="ros2msg", definition=locally_installed_ros2msg(topic_metadata.type)
        )
        for topic_metadata in data_source.get_all_topics_and_types()
    }


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ROS2 sqlite3 bags."""

    def struct(self, topic: str, data_source: rosbag2_py.SequentialReader) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        type_name = self.native_type_name(topic, data_source)
        definition = message_definitions(data_source)[type_name]
        match definition.encoding:
            case "ros2msg":
                main, deps = parse.parse(definition.definition.decode("utf-8"))
                return schema.to_pa_struct(main, deps)
            case _:
                raise base.UnsupportedEncodingError(definition.encoding)

    def describe(self, topic: str, data_source: rosbag2_py.SequentialReader) -> str:
        """Return a human-readable description of the given topic."""
        type_name = self.native_type_name(topic, data_source)
        definition = message_definitions(data_source)[type_name]
        match definition.encoding:
            case "ros2msg":
                return definition.definition.decode("utf-8")
            case _:
                raise base.UnsupportedEncodingError(definition.encoding)

    def _metadata(self, data_source: rosbag2_py.SequentialReader) -> rosbag2_py.BagMetadata:
        return data_source.get_metadata()


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
