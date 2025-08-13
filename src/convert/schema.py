"""Utilities for retrieving schema encodings and strings from a robolog."""

import functools
import pathlib
from enum import Enum
from typing import Protocol

import yaml

from src import robolog


class UnsupportedSchemaEncodingError(Exception):
    """Raised when a schema encoding is not supported."""


class SchemaNotFoundError(Exception):
    """Raised when a schema of a message type is not found in the robolog."""


class Encoding(Enum):
    """Represent supported schema encodings of message types in a robolog."""

    ROS1MSG = "ros1msg"  # .bag, .mcap
    ROS2MSG = "ros2msg"  # .db3, .mcap
    PROTOBUF = "protobuf"  # .mcap
    PX4ULOG = "px4ulog"  # .ulg
    ARDUPILOTBIN = "ardupilotbin"  # .bin


class Ros2Schema(Protocol):
    """Duck typing of ROS2's mcap.records.Schema.

    See: https://github.com/foxglove/mcap/blob/main/python/mcap/mcap/records.py

    """

    id: str
    data: bytes
    encoding: str
    name: str


@functools.lru_cache(maxsize=128)
def _schemas_from_mcap(robolog_path: str | pathlib.Path) -> dict[str, Ros2Schema]:
    """Return a dictionary of message type names to mcap's Schema objects from .mcap files."""
    from mcap import reader

    path = pathlib.Path(robolog_path).absolute()

    match robolog.detect_robolog_type(path):
        case robolog.RobologType.ROS2_MCAP_FILE:
            with open(path, "rb") as stream:
                summary = reader.make_reader(stream).get_summary()
                return {s.name: s for s in summary.schemas.values()}

        case robolog.RobologType.ROS2_MCAP_DIR:
            metadata = yaml.safe_load((path / "metadata.yaml").read_text())
            schemas = {}
            for mcap_file in metadata["rosbag2_bagfile_information"]["relative_file_paths"]:
                schemas = {**schemas, **_schemas_from_mcap(path / mcap_file)}
            return schemas

        case _:
            raise robolog.UnsupportedRobologTypeError(robolog_path)


@functools.lru_cache(maxsize=128)
def _ros1msg_strings_from_bag(robolog_path: str | pathlib.Path) -> dict[str, str]:
    """Return a dictionary of message type names to ros1msg strings from a .bag file."""
    import rosbag

    with rosbag.Bag(robolog_path, allow_unindexed=True) as bag:
        type_names_to_topics = {}
        for topic, tup in bag.get_type_and_topic_info().topics.items():
            type_names_to_topics[tup.msg_type] = topic
        schemas = {}
        for type_name, topic in type_names_to_topics.items():
            _, msg, _ = next(bag.read_messages([topic]))
            schemas[type_name] = msg._full_text
        return schemas


@functools.lru_cache(maxsize=128)
def _ros2msg_string_from_local(type_name: str, separator: str = "=" * 80) -> str:
    """Return the ros2msg string of the given message type from locally installed packages.

    If packages are not installed or sourced locally, this will throw an error.

    """
    import collections

    from rosidl_parser.definition import NamespacedType
    from rosidl_runtime_py import get_interface_path
    from rosidl_runtime_py.utilities import get_message

    def _resolve_type_name(name: str) -> str:
        match tuple(name.split("/")):
            case (package, "msg", class_):
                return name
            case (package, class_):
                return f"{package}/msg/{class_}"
            case _:
                raise ValueError(f"Invalid type name: {name}")

    visited = set()
    dependencies = []
    stack = collections.deque([_resolve_type_name(type_name)])
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

    return f"\n{separator}\n".join(sections)


@functools.lru_cache(maxsize=128)
def _px4ulog_strings_from_ulg(robolog_path: str | pathlib.Path) -> dict[str, str]:
    """Return a dictionary of message type names to px4ulog strings from a .ulg file."""
    from pyulog import core

    ulog = core.ULog(str(robolog_path), parse_header_only=False)
    schemas = {}
    for type_name, multi_id in [(data.name, data.multi_id) for data in ulog.data_list]:
        topic_data = ulog.get_dataset(type_name, multi_id)
        schema = {field.field_name: field.type_str for field in topic_data.field_data}
        schemas[type_name] = yaml.dump(schema)
    return schemas


@functools.lru_cache(maxsize=128)
def _ardupilot_strings_from_bin(robolog_path: str | pathlib.Path) -> dict[str, str]:
    """Return a dictionary of message type names to ArduPilot format strings from a .bin file."""
    from pymavlink import DFReader

    df_reader = DFReader.DFReader_binary(str(robolog_path))
    schemas = {}
    while msg := df_reader.recv_match(type="FMT"):
        schema = {
            field_name: field_type
            for field_name, field_type in zip(msg.Columns.split(","), msg.Format, strict=True)
        }
        schemas[msg.Name] = yaml.dump(schema)
    return schemas


def schema_encoding(robolog_path: str | pathlib.Path, type_name: str) -> Encoding:
    """Return the schema encoding of the given message type in the robolog."""
    path = pathlib.Path(robolog_path).absolute()

    match robolog.detect_robolog_type(path):
        case robolog.RobologType.ROS1_BAG_FILE:
            return Encoding.ROS1MSG

        case robolog.RobologType.ROS2_DB3_FILE | robolog.RobologType.ROS2_DB3_DIR:
            return Encoding.ROS2MSG

        case robolog.RobologType.ROS2_MCAP_FILE | robolog.RobologType.ROS2_MCAP_DIR:
            try:
                encoding = _schemas_from_mcap(path)[type_name].encoding
                return Encoding(encoding)
            except KeyError as err:
                raise SchemaNotFoundError(type_name) from err
            except ValueError as err:
                raise UnsupportedSchemaEncodingError(f"{type_name}: {encoding}") from err

        case robolog.RobologType.PX4_ULG_FILE:
            return Encoding.PX4ULOG

        case robolog.RobologType.ARDUPILOT_BIN_FILE:
            return Encoding.ARDUPILOTBIN

        case _:
            raise robolog.UnsupportedRobologTypeError(robolog_path)


def schema_string(  # noqa: C901, PLR0912
    robolog_path: str | pathlib.Path, type_name: str
) -> str | bytes:
    """Return the schema string/bytes of the given message type in the robolog."""
    path = pathlib.Path(robolog_path).absolute()

    match robolog.detect_robolog_type(path):
        case robolog.RobologType.ROS1_BAG_FILE:
            try:
                return _ros1msg_strings_from_bag(path)[type_name]
            except KeyError as err:
                raise SchemaNotFoundError(type_name) from err

        case robolog.RobologType.ROS2_DB3_FILE | robolog.RobologType.ROS2_DB3_DIR:
            return _ros2msg_string_from_local(type_name)

        case robolog.RobologType.ROS2_MCAP_FILE | robolog.RobologType.ROS2_MCAP_DIR:
            try:
                string_or_bytes = _schemas_from_mcap(path)[type_name].data
            except KeyError as err:
                raise SchemaNotFoundError(type_name) from err

            match schema_encoding(robolog_path, type_name):
                case Encoding.ROS1MSG | Encoding.ROS2MSG:
                    return string_or_bytes.decode("utf-8")
                case Encoding.PROTOBUF:
                    return string_or_bytes
                case encoding:
                    raise UnsupportedSchemaEncodingError(f"{type_name}: {encoding}")

        case robolog.RobologType.PX4_ULG_FILE:
            try:
                return _px4ulog_strings_from_ulg(path)[type_name]
            except KeyError as err:
                raise SchemaNotFoundError(type_name) from err

        case robolog.RobologType.ARDUPILOT_BIN_FILE:
            try:
                return _ardupilot_strings_from_bin(path)[type_name]
            except KeyError as err:
                raise SchemaNotFoundError(type_name) from err

        case _:
            raise robolog.UnsupportedRobologTypeError(robolog_path)
