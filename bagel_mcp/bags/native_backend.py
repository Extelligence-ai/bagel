"""ROS bag I/O through the native ROS stacks of the service images.

``rosbag`` / ``genpy`` (ROS 1) and ``rosbag2_py`` / ``rclpy`` / ``rosidl`` (ROS 2) are apt
packages sourced from ``/opt/ros``; this backend exists where they are, for the parity
tests that hold the ``rosbags`` backend to the native results, and as an escape hatch
(``BAG_BACKEND=native``). Everything here is what Bagel did before the interface.
"""

from __future__ import annotations

import collections
import functools
import pathlib
from collections.abc import Iterator
from typing import Any

import yaml

from bagel_mcp import ros_native
from bagel_mcp.bags import base
from bagel_mcp.source.ros2 import decompress

FEATURE = "The native ROS bag backend"
rosbag = ros_native.optional("rosbag", feature=FEATURE)
genpy = ros_native.optional("genpy", feature=FEATURE)
genpy_dynamic = ros_native.optional("genpy.dynamic", feature=FEATURE)
rosbag2_py = ros_native.optional("rosbag2_py", feature=FEATURE)
rclpy_serialization = ros_native.optional("rclpy.serialization", feature=FEATURE)
rosidl_definition = ros_native.optional("rosidl_parser.definition", feature=FEATURE)
rosidl_runtime = ros_native.optional("rosidl_runtime_py", feature=FEATURE)
rosidl_utilities = ros_native.optional("rosidl_runtime_py.utilities", feature=FEATURE)


@functools.lru_cache
def locally_installed_ros2msg(type_name: str) -> str:
    """Return the full-text definition of a type from the locally installed ROS 2 packages.

    Dependencies follow the main message under ``MSG:`` headers, the same layout MCAP
    schemas and rosbags' typestores use. Raises if the package is not installed here.
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
        for slot_type in rosidl_utilities.get_message(current).SLOT_TYPES:
            if isinstance(slot_type, rosidl_definition.NamespacedType):
                stack.append("/".join(slot_type.namespaced_name()))

    sections = []
    for dependency_type_name in dependencies:
        msg_file = rosidl_runtime.get_interface_path(dependency_type_name)
        section = pathlib.Path(msg_file).read_text(encoding="utf-8")
        if sections:
            section = f"MSG: {dependency_type_name}\n{section}"
        sections.append(section)

    separator = "=" * 80
    return f"\n{separator}\n".join(sections)


def _ros2_decompressed(path: pathlib.Path) -> pathlib.Path:
    """Expand zstd-compressed storage files next to the originals, as rosbag2_py needs."""
    path = decompress.ros2bag(path)
    if path.is_dir():
        metadata = rosbag2_py.Info().read_metadata(str(path), "")
        if metadata.compression_format == "zstd":
            import zstandard

            for rel_file, file_info in zip(
                metadata.relative_file_paths, metadata.files, strict=True
            ):
                target = path / file_info.path
                if not target.exists():
                    with open(path / rel_file, "rb") as f_in, open(target, "wb") as f_out:
                        zstandard.ZstdDecompressor().copy_stream(f_in, f_out)
    return path


class Reader:
    """A bag open through the native stack; see :class:`bagel_mcp.bags.base.Reader`."""

    def __init__(self, path: pathlib.Path, ros_version: int) -> None:
        """Open the bag at ``path`` with the native stack of this ROS version."""
        self._ros_version = ros_version
        self._path = path
        if ros_version == 1:
            self._bag = rosbag.Bag(f=str(path), mode="r", allow_unindexed=True)
            self._yaml = yaml.safe_load(self._bag._get_yaml_info())
            self._metadata = None
        else:
            self._path = _ros2_decompressed(path)
            self._metadata = rosbag2_py.Info().read_metadata(str(self._path), "")
        self.info = self._info(path)

    # -- metadata -----------------------------------------------------------------

    def _info(self, path: pathlib.Path) -> base.BagInfo:
        if self._ros_version == 1:
            info = self._yaml
            types = {entry["type"]: entry["md5"] for entry in info.get("types", [])}
            topics = [
                base.TopicInfo(
                    name=entry["topic"],
                    type_name=entry["type"],
                    message_count=entry["messages"],
                    serialization_format="ros1",
                    definition=None,
                    digest=types.get(entry["type"], ""),
                )
                for entry in info.get("topics", [])
            ]
            # Exact stamps from the index: get_start_time()/get_end_time() go through
            # floats and can lose the last nanosecond.
            stamps = [
                entry.time.to_nsec()
                for entries in getattr(self._bag, "_connection_indexes", {}).values()
                for entry in entries
            ]
            start_ns = min(stamps) if stamps else 0
            end_ns = max(stamps) if stamps else 0
            return base.BagInfo(
                path=path,
                ros_version=1,
                version=str(info["version"]),
                storage_identifier="rosbag1",
                compression_format=str(info["compression"]),
                compression_mode="",
                relative_file_paths=[path.name],
                files=[base.FileInfo(path.name, info["messages"], start_ns, end_ns - start_ns)],
                topics=topics,
                message_count=info["messages"],
                start_ns=start_ns,
                end_ns=end_ns,
                size_bytes=int(info["size"]),
            )

        metadata = self._metadata
        topics = [
            base.TopicInfo(
                name=entry.topic_metadata.name,
                type_name=entry.topic_metadata.type,
                message_count=entry.message_count,
                serialization_format=entry.topic_metadata.serialization_format,
                definition=None,
                digest=getattr(entry.topic_metadata, "type_description_hash", "") or "",
            )
            for entry in metadata.topics_with_message_count
        ]
        return base.BagInfo(
            path=path,
            ros_version=2,
            version=str(metadata.version),
            storage_identifier=metadata.storage_identifier,
            compression_format=metadata.compression_format,
            compression_mode=metadata.compression_mode,
            relative_file_paths=list(metadata.relative_file_paths),
            files=[
                base.FileInfo(
                    path=file.path,
                    message_count=file.message_count,
                    start_ns=file.starting_time.nanoseconds,
                    duration_ns=int(file.duration.total_seconds() * base.NANOSECONDS_PER_SECOND),
                )
                for file in metadata.files
            ],
            topics=topics,
            message_count=metadata.message_count,
            start_ns=metadata.starting_time.nanoseconds,
            end_ns=metadata.starting_time.nanoseconds + metadata.duration.nanoseconds,
            size_bytes=metadata.bag_size,
        )

    # -- definitions -----------------------------------------------------------------

    def definition(self, topic: str) -> str:
        """ROS 1: the recorded connection's full text; ROS 2: the locally installed interface."""
        if self._ros_version == 1:
            _, message, _ = next(self._bag.read_messages([topic]))
            return message._full_text
        return locally_installed_ros2msg(self.info.topic(topic).type_name)

    # -- messages -----------------------------------------------------------------

    def _ros2_reader(self) -> Any:  # noqa: ANN401 -- rosbag2_py.SequentialReader
        reader = rosbag2_py.SequentialReader()
        reader.open(
            rosbag2_py.StorageOptions(uri=str(self._path), storage_id=self.info.storage_identifier),
            rosbag2_py.ConverterOptions(
                input_serialization_format="", output_serialization_format=""
            ),
        )
        return reader

    def _ros2_records(
        self, topics: list[str], start_seconds: float | None, end_seconds: float | None
    ) -> Iterator[tuple[str, int, bytes, str]]:
        reader = self._ros2_reader()
        reader.set_filter(rosbag2_py.StorageFilter(topics))
        if start_seconds is not None:
            reader.seek(base.ros2_nanoseconds(start_seconds))
        type_names = {entry.name: entry.type for entry in reader.get_all_topics_and_types()}
        while reader.has_next():
            topic, data, timestamp_ns = reader.read_next()
            if end_seconds is not None and base.ros2_seconds(timestamp_ns) > end_seconds:
                return
            yield topic, timestamp_ns, data, type_names[topic]

    def raw_messages(
        self,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, int, bytes]]:
        """Yield ``(topic, timestamp_ns, serialized bytes)`` in time order."""
        if self._ros_version == 1:
            records = self._bag.read_messages(
                topics,
                genpy.Time.from_sec(start_seconds_inclusive) if start_seconds_inclusive else None,
                genpy.Time.from_sec(end_seconds_inclusive) if end_seconds_inclusive else None,
                raw=True,
            )
            for topic, raw, timestamp in records:
                yield topic, timestamp.to_nsec(), raw[1]
            return
        for topic, timestamp_ns, data, _ in self._ros2_records(
            topics, start_seconds_inclusive, end_seconds_inclusive
        ):
            yield topic, timestamp_ns, bytes(data)

    def messages(
        self,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, int, object]]:
        """Yield ``(topic, timestamp_ns, deserialized message)`` in time order."""
        if self._ros_version == 1:
            records = self._bag.read_messages(
                topics,
                genpy.Time.from_sec(start_seconds_inclusive) if start_seconds_inclusive else None,
                genpy.Time.from_sec(end_seconds_inclusive) if end_seconds_inclusive else None,
            )
            for topic, message, timestamp in records:
                yield topic, timestamp.to_nsec(), message
            return
        for topic, timestamp_ns, data, type_name in self._ros2_records(
            topics, start_seconds_inclusive, end_seconds_inclusive
        ):
            message_class = rosidl_utilities.get_message(type_name)
            yield topic, timestamp_ns, rclpy_serialization.deserialize_message(data, message_class)

    def close(self) -> None:
        """Release the underlying files."""
        if self._ros_version == 1:
            self._bag.close()


def _topic_metadata(topic_id: int, name: str, type_name: str, serialization_format: str) -> Any:  # noqa: ANN401 -- rosbag2_py.TopicMetadata
    """Build a ``TopicMetadata`` on any supported ROS 2 distro.

    ``rosbag2_py.TopicMetadata`` grew a required ``id`` argument after Humble; Humble's
    constructor rejects the kwarg outright.
    """
    try:  # Iron and newer require an id
        return rosbag2_py.TopicMetadata(
            id=topic_id, name=name, type=type_name, serialization_format=serialization_format
        )
    except TypeError:  # Humble predates the id field
        return rosbag2_py.TopicMetadata(
            name=name, type=type_name, serialization_format=serialization_format
        )


class Writer:
    """A bag being written through the native stack; see :class:`bagel_mcp.bags.base.Writer`."""

    def __init__(
        self,
        path: pathlib.Path,
        ros_version: int,
        storage: str = "sqlite3",
        compression: tuple[str, str] | str | None = None,
    ) -> None:
        """Create the bag at ``path`` with the native stack of this ROS version."""
        self._ros_version = ros_version
        self._topics: dict[str, base.TopicInfo] = {}
        self._types: dict[str, Any] = {}
        if compression:
            raise NotImplementedError("The native backend writes uncompressed bags only.")
        if ros_version == 1:
            self._writer: Any = rosbag.Bag(str(path), mode="w")
        else:
            self._writer = rosbag2_py.SequentialWriter()
            self._writer.open(
                rosbag2_py.StorageOptions(uri=str(path), storage_id=storage),
                rosbag2_py.ConverterOptions("", ""),
            )

    def add_topic(self, topic: base.TopicInfo) -> None:
        """Declare a topic before writing to it."""
        self._topics[topic.name] = topic
        if self._ros_version == 1:
            if not topic.definition:
                raise ValueError(
                    f"A ROS 1 topic needs its message definition to be written: {topic.name}"
                )
            classes = genpy_dynamic.generate_dynamic(topic.type_name, topic.definition)
            self._types[topic.name] = classes[topic.type_name]
            return
        self._writer.create_topic(
            _topic_metadata(
                len(self._topics) - 1, topic.name, topic.type_name, topic.serialization_format
            )
        )

    def write(self, topic: str, timestamp_ns: int, data: bytes) -> None:
        """Append one serialized message."""
        if self._ros_version == 1:
            info = self._topics[topic]
            self._writer.write(
                topic,
                (info.type_name, data, info.digest, self._types[topic]),
                genpy.Time(*divmod(timestamp_ns, base.NANOSECONDS_PER_SECOND)),
                raw=True,
            )
            return
        self._writer.write(topic, data, timestamp_ns)

    def close(self) -> None:
        """Finalize the bag."""
        if self._ros_version == 1:
            self._writer.close()
        else:
            del self._writer

    def __enter__(self) -> Writer:
        """Return self."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Close the bag."""
        self.close()


def describe_backend() -> dict[str, str]:
    """Name the library behind this backend, for diagnostics and parity reports."""
    return {"backend": "native"}
