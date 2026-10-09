"""ROS bag I/O through the pure-Python ``rosbags`` library (the ``ros`` extra).

Reads ROS 1 ``.bag`` files (uncompressed, bz2, lz4) and ROS 2 bags (sqlite3 and MCAP
storage, single files or directories, file- and message-level zstd compression);
writes ROS 1 bags and rosbag2 bags that ``rosbag info`` / ``ros2 bag info`` open.
No ROS installation is involved: message types come from the bag's own embedded
definitions when it carries them, from the locally installed ROS 2 interfaces when
the process happens to run inside a service image, and from rosbags' bundled
typestores otherwise.
"""

from __future__ import annotations

import logging
import pathlib
from collections.abc import Iterator
from typing import Any

import lz4.frame
import yaml
from rosbags.interfaces import MessageDefinitionFormat
from rosbags.rosbag1 import Reader as Rosbag1Reader
from rosbags.rosbag1 import Writer as Rosbag1Writer
from rosbags.rosbag2 import CompressionFormat, CompressionMode, StoragePlugin
from rosbags.rosbag2 import Reader as Rosbag2Reader
from rosbags.rosbag2 import Writer as Rosbag2Writer
from rosbags.typesys import (
    Stores,
    TypesysError,
    get_types_from_idl,
    get_types_from_msg,
    get_typestore,
)

from bagel_mcp.bags import base
from bagel_mcp.bags.base import BagStorageError
from bagel_mcp.source.ros2 import decompress

_ROS2_STORES = {
    "humble": Stores.ROS2_HUMBLE,
    "iron": Stores.ROS2_IRON,
    "jazzy": Stores.ROS2_JAZZY,
    "kilted": Stores.ROS2_KILTED,
    "lyrical": Stores.ROS2_LYRICAL,
}
# rosbag2 metadata version this writer emits. Humble and Iron decode
# `offered_qos_profiles` as a string, which version 9 turns into a sequence; 8
# is the newest layout every supported distro's `ros2 bag info` still reads.
_ROSBAG2_WRITER_VERSION = 8
_ROS1_COMPRESSION_BY_MODULE = {"bz2": "bz2", "lz4": "lz4"}
ROS2 = 2


def _store(ros_version: int, distro: str | None) -> Stores:
    if ros_version == 1:
        return Stores.ROS1_NOETIC
    return _ROS2_STORES.get(distro or "", Stores.LATEST)


def _register(typestore: Any, msgtype: str, msgdef: tuple[Any, str]) -> None:  # noqa: ANN401
    """Teach the typestore a bag-embedded definition so custom types deserialize."""
    fmt, text = msgdef
    if not text:
        return
    try:
        if fmt == MessageDefinitionFormat.MSG:
            typestore.register(get_types_from_msg(text, msgtype))
        elif fmt == MessageDefinitionFormat.IDL:
            typestore.register(get_types_from_idl(text))
    except TypesysError as error:  # a definition the bundled store already has, differently
        logging.debug("Keeping the bundled definition of %s: %s", msgtype, error)


def _native_ros2_definition(msgtype: str) -> str | None:
    """Return the locally installed interface's full text, inside a ROS 2 image."""
    from bagel_mcp import ros_native

    if not ros_native.available("rosidl_runtime_py"):
        return None
    from bagel_mcp.bags import native_backend

    try:
        return native_backend.locally_installed_ros2msg(msgtype)
    except Exception as error:  # the type is not installed here; fall through to the store
        logging.debug("No local interface for %s: %s", msgtype, error)
        return None


class Reader:
    """A bag open through rosbags; see :class:`bagel_mcp.bags.base.Reader`."""

    def __init__(self, path: pathlib.Path, ros_version: int) -> None:
        """Open the bag at ``path`` (ROS 1 ``.bag``, or a ROS 2 directory or storage file)."""
        self._ros_version = ros_version
        if ros_version == 1:
            self._reader: Any = Rosbag1Reader(path)
        else:
            self._reader = Rosbag2Reader(decompress.ros2bag(path))
        try:
            self._reader.open()
        except Exception as error:
            raise BagStorageError(str(error)) from error
        distro = None if ros_version == 1 else self._reader.ros_distro
        self._typestore = get_typestore(_store(ros_version, distro))
        for connection in self._reader.connections:
            _register(self._typestore, connection.msgtype, connection.msgdef)
        self.info = self._info(path)

    # -- metadata -----------------------------------------------------------------

    def _topics(self) -> list[base.TopicInfo]:
        topics = []
        for connection in self._reader.connections:
            fmt, text = connection.msgdef
            embedded = text if text and fmt == MessageDefinitionFormat.MSG else None
            if self._ros_version == 1:
                topics.append(
                    base.TopicInfo(
                        name=connection.topic,
                        type_name=base.ros1_type_name(connection.msgtype),
                        message_count=connection.msgcount,
                        serialization_format="ros1",
                        definition=embedded,
                        digest=connection.digest or "",
                    )
                )
            else:
                topics.append(
                    base.TopicInfo(
                        name=connection.topic,
                        type_name=connection.msgtype,
                        message_count=connection.msgcount,
                        serialization_format=connection.ext.serialization_format,
                        definition=embedded,
                        digest=connection.digest or "",
                    )
                )
        return topics

    def _info(self, path: pathlib.Path) -> base.BagInfo:
        reader = self._reader
        if self._ros_version == 1:
            # rosbags' rosbag1 end_time is exclusive (last stamp + 1 ns); `rosbag` and
            # the bag format report the last stamp itself, so the interface does too.
            end_ns = reader.end_time - 1 if reader.message_count else reader.start_time
            duration_ns = end_ns - reader.start_time
            modules = {
                chunk.decompressor.__module__.split(".")[0] for chunk in reader.chunks.values()
            }
            compression = next(
                (name for module, name in _ROS1_COMPRESSION_BY_MODULE.items() if module in modules),
                "none",
            )
            return base.BagInfo(
                path=path,
                ros_version=1,
                version="2.0",
                storage_identifier="rosbag1",
                compression_format=compression,
                compression_mode="",
                relative_file_paths=[path.name],
                files=[
                    base.FileInfo(path.name, reader.message_count, reader.start_time, duration_ns)
                ],
                topics=self._topics(),
                message_count=reader.message_count,
                start_ns=reader.start_time,
                end_ns=end_ns,
                size_bytes=path.stat().st_size,
            )

        version, relative_paths, files = "", [], []
        start_ns, end_ns = reader.start_time, reader.start_time
        metadata_file = path / "metadata.yaml" if path.is_dir() else None
        if metadata_file is not None and metadata_file.exists():
            metadata = yaml.safe_load(metadata_file.read_text(encoding="utf-8"))
            metadata = metadata["rosbag2_bagfile_information"]
            version = str(metadata.get("version", ""))
            # The bounds `ros2 bag info` reports: starting_time and duration as recorded.
            start_ns = int(metadata["starting_time"]["nanoseconds_since_epoch"])
            end_ns = start_ns + int(metadata["duration"]["nanoseconds"])
            relative_paths = list(metadata.get("relative_file_paths", []))
            for entry in metadata.get("files", []):
                files.append(
                    base.FileInfo(
                        path=entry["path"],
                        message_count=entry["message_count"],
                        start_ns=entry["starting_time"]["nanoseconds_since_epoch"],
                        duration_ns=entry["duration"]["nanoseconds"],
                    )
                )
        if not relative_paths:
            relative_paths = [path.name] if path.is_file() else []
        storage = (
            "mcap"
            if any(".mcap" in pathlib.PurePath(name).suffixes for name in relative_paths)
            else "sqlite3"
        )
        if metadata_file is None or not metadata_file.exists():
            # A lone storage file: rosbags' end_time is exclusive (last stamp + 1 ns)
            # for every storage plugin; the bag format reports the last stamp itself.
            end_ns = reader.end_time - 1 if reader.message_count else reader.start_time
        if not files:
            files = [
                base.FileInfo(name, reader.message_count, start_ns, end_ns - start_ns)
                for name in relative_paths
            ]
        size = (
            path.stat().st_size
            if path.is_file()
            else sum(file.stat().st_size for file in path.iterdir() if file.is_file())
        )
        return base.BagInfo(
            path=path,
            ros_version=2,
            version=version,
            storage_identifier=storage,
            compression_format=reader.compression_format or "",
            compression_mode=reader.compression_mode or "",
            relative_file_paths=relative_paths,
            files=files,
            topics=self._topics(),
            message_count=reader.message_count,
            start_ns=start_ns,
            end_ns=end_ns,
            size_bytes=size,
        )

    # -- definitions -----------------------------------------------------------------

    def _connection(self, topic: str) -> Any:  # noqa: ANN401 -- rosbags Connection
        for connection in self._reader.connections:
            if connection.topic == topic:
                return connection
        raise base.UnknownTopicError(topic)

    def definition(self, topic: str) -> str:
        """Return the type's full text: embedded in the bag, else local ROS 2, else bundled."""
        connection = self._connection(topic)
        fmt, text = connection.msgdef
        if text and fmt == MessageDefinitionFormat.MSG:
            return text
        if self._ros_version == ROS2:
            local = _native_ros2_definition(connection.msgtype)
            if local is not None:
                return local
        generated, _ = self._typestore.generate_msgdef(
            connection.msgtype, ros_version=self._ros_version
        )
        return generated

    # -- messages -----------------------------------------------------------------

    def _bounds(self, start: float | None, end: float | None) -> tuple[int | None, int | None]:
        if self._ros_version == 1:
            return base.ros1_nanoseconds(start), base.ros1_nanoseconds(end)
        return base.ros2_nanoseconds(start), None

    def _records(
        self, topics: list[str], start_seconds: float | None, end_seconds: float | None
    ) -> Iterator[tuple[Any, int, bytes]]:
        wanted = set(topics)
        connections = [c for c in self._reader.connections if c.topic in wanted]
        if not connections:
            return
        start_ns, end_ns = self._bounds(start_seconds, end_seconds)
        for connection, timestamp_ns, data in self._reader.messages(connections, start=start_ns):
            if end_ns is not None and timestamp_ns > end_ns:
                return
            if (
                end_ns is None
                and end_seconds is not None
                and base.ros2_seconds(timestamp_ns) > end_seconds
            ):
                return
            yield connection, timestamp_ns, data

    def raw_messages(
        self,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, int, bytes]]:
        """Yield ``(topic, timestamp_ns, serialized bytes)`` in time order."""
        for connection, timestamp_ns, data in self._records(
            topics, start_seconds_inclusive, end_seconds_inclusive
        ):
            yield connection.topic, timestamp_ns, bytes(data)

    def messages(
        self,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, int, object]]:
        """Yield ``(topic, timestamp_ns, deserialized message)`` in time order."""
        deserialize = (
            self._typestore.deserialize_ros1
            if self._ros_version == 1
            else self._typestore.deserialize_cdr
        )
        for connection, timestamp_ns, data in self._records(
            topics, start_seconds_inclusive, end_seconds_inclusive
        ):
            yield connection.topic, timestamp_ns, deserialize(data, connection.msgtype)

    def close(self) -> None:
        """Release the underlying files."""
        self._reader.close()


def ros1_lz4_compress(data: bytes) -> bytes:
    """Compress a rosbag1 chunk into an lz4 frame that ``roslz4`` can read back.

    `rosbags` writes lz4 frames with linked blocks and a stored content size; the
    ROS 1 stack's ``roslz4`` decoder supports neither and rejects such chunks as
    malformed. Independent blocks without the size field are what ``roslz4`` itself
    writes, and every lz4 frame decoder reads them.
    """
    return lz4.frame.compress(data, block_linked=False, store_size=False)


class Writer:
    """A bag being written through rosbags; see :class:`bagel_mcp.bags.base.Writer`."""

    def __init__(
        self,
        path: pathlib.Path,
        ros_version: int,
        storage: str = "sqlite3",
        compression: tuple[str, str] | str | None = None,
    ) -> None:
        """Create the bag at ``path``, which must not exist yet."""
        self._ros_version = ros_version
        self._connections: dict[str, Any] = {}
        if ros_version == 1:
            self._writer: Any = Rosbag1Writer(path)
            if compression:
                self._writer.set_compression(
                    Rosbag1Writer.CompressionFormat[str(compression).upper()]
                )
                if str(compression).lower() == "lz4":
                    self._writer.compressor = ros1_lz4_compress
            self._typestore = get_typestore(Stores.ROS1_NOETIC)
        else:
            plugin = {"sqlite3": StoragePlugin.SQLITE3, "mcap": StoragePlugin.MCAP}[storage]
            self._writer = Rosbag2Writer(
                path, version=_ROSBAG2_WRITER_VERSION, storage_plugin=plugin
            )
            if compression:
                mode, fmt = compression
                self._writer.set_compression(
                    CompressionMode[mode.upper()], CompressionFormat[fmt.upper()]
                )
            self._typestore = get_typestore(Stores.LATEST)
        self._writer.open()

    def add_topic(self, topic: base.TopicInfo) -> None:
        """Declare a topic, carrying the source's definition and digest into the output."""
        msgtype = base.rosbags_type_name(topic.type_name)
        if topic.definition:
            _register(self._typestore, msgtype, (MessageDefinitionFormat.MSG, topic.definition))
        if self._ros_version == 1:
            connection = self._writer.add_connection(
                topic.name,
                msgtype,
                typestore=self._typestore,
                msgdef=topic.definition,
                md5sum=topic.digest or None,
            )
        elif topic.definition and topic.digest.startswith("RIHS01_"):
            connection = self._writer.add_connection(
                topic.name,
                msgtype,
                msgdef=topic.definition,
                rihs01=topic.digest,
                serialization_format=topic.serialization_format,
            )
        else:
            connection = self._writer.add_connection(
                topic.name,
                msgtype,
                typestore=self._typestore,
                serialization_format=topic.serialization_format,
            )
        self._connections[topic.name] = connection

    def write(self, topic: str, timestamp_ns: int, data: bytes) -> None:
        """Append one serialized message."""
        self._writer.write(self._connections[topic], timestamp_ns, data)

    def close(self) -> None:
        """Finalize the bag (indexes, metadata.yaml)."""
        self._writer.close()

    def __enter__(self) -> Writer:
        """Return self."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Close the bag."""
        self.close()


def describe_backend() -> dict[str, str]:
    """Name the library behind this backend, for diagnostics and parity reports."""
    import importlib.metadata

    return {"backend": "rosbags", "rosbags": importlib.metadata.version("rosbags")}
