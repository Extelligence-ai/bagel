"""File-backed topic buffer writer and reader."""

import json
import pathlib
import pickle
import shutil
import tempfile
import time
import uuid
from collections.abc import Iterator
from typing import Any

import filelock
import pyarrow as pa
import yaml

from settings import settings


def _topic_uuid(topic: str) -> str:
    """Generate a UUID for a given topic name."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, topic))


def _artifact_paths(path: pathlib.Path, topic: str) -> dict[str, pathlib.Path]:
    """Return the artifact paths for a given topic under the base path."""
    topic_uuid = _topic_uuid(topic)
    data_directory = path / "data" / topic_uuid
    return {
        "metadata_file": path / "metadata" / f"{topic_uuid}.yaml",
        "definition_file": path / "definitions" / f"{topic_uuid}.txt",
        "struct_file": path / "structs" / f"{topic_uuid}.pkl",
        "data_directory": data_directory,
        "current_data_file": data_directory / "current.jsonl",
        "overflow_data_file": data_directory / "overflow.jsonl",
        "file_lock": data_directory / "file.lock",
    }


class TopicBufferWriter:
    """A buffer writer for messages from a specific topic.

    Key features:
    - File-backed: Messages are serialized into line-delimited JSON files on disk.
    - Thread-safe: Uses file locks to ensure safe concurrent writes.
    - Self-describing: Stores metadata, message definition, and schema alongside message data.

    """

    def __init__(  # noqa: PLR0913
        self,
        path: pathlib.Path,
        topic: str,
        type_name: str,
        definition: str,
        struct: pa.StructType,
        buffer_size_bytes: int | None = settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES,
    ) -> None:
        """Initialize a TopicBufferWriter instance.

        Args:
            path (pathlib.Path): The base path for the topic buffer.
            topic (str): The name of the topic.
            type_name (str): The native type name of the topic.
            definition (str): The message definition of the topic.
            struct (pa.StructType): The PyArrow StructType of the topic.
            buffer_size_bytes (int | None, optional): The maximum buffer size in bytes.
                If None, the buffer size is unlimited till disk space is exhausted.

        """
        self._topic = topic
        self._type_name = type_name
        self._definition = definition
        self._struct = struct
        self._buffer_size_bytes = buffer_size_bytes
        self._created_at = time.time()

        metadata = {
            "topic": self._topic,
            "type_name": self._type_name,
            "created_at": self._created_at,
            "buffer_size_bytes": self._buffer_size_bytes,
        }

        paths = _artifact_paths(path, topic)
        self._metadata_file = paths["metadata_file"]
        self._definition_file = paths["definition_file"]
        self._struct_file = paths["struct_file"]
        self._data_directory = paths["data_directory"]
        self._current_data_file = paths["current_data_file"]
        self._overflow_data_file = paths["overflow_data_file"]

        self._write_metadata(metadata)
        self._write_definition(self._definition)
        self._write_struct(self._struct)
        self._data_directory.mkdir(parents=True, exist_ok=True)
        self._file_lock = filelock.FileLock(paths["file_lock"])

    @property
    def topic(self) -> str:
        """Topic name for the messages held by this buffer."""
        return self._topic

    def append(self, msg: dict[str, Any]) -> None:
        """Append a message to the buffer."""
        record = {
            settings.TIMESTAMP_SECONDS_COLUMN_NAME: time.time(),
            self._topic: msg,
        }
        line = json.dumps(record) + "\n"
        with self._file_lock:
            if (
                self._buffer_size_bytes
                and self._current_data_file.exists()
                and (
                    self._current_data_file.stat().st_size + len(line.encode("utf-8"))
                    > self._buffer_size_bytes
                )
            ):
                self._overflow_data_file.unlink(missing_ok=True)
                self._current_data_file.rename(self._overflow_data_file)
            with open(self._current_data_file, "a", encoding="utf-8") as f:
                f.write(line)

    def _write_metadata(self, metadata: dict[str, Any]) -> None:
        self._metadata_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._metadata_file, "w", encoding="utf-8") as f:
            f.write(yaml.safe_dump(metadata))

    def _write_definition(self, definition: str) -> None:
        self._definition_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._definition_file, "w", encoding="utf-8") as f:
            f.write(definition)

    def _write_struct(self, struct: pa.StructType) -> None:
        self._struct_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._struct_file, "wb") as f:
            pickle.dump(struct, f)


class TopicBufferReader:
    """A reader for consuming messages from a file-backed topic buffer.

    Each reader instance corresponds to a single "topic" and provides access
    to messages written by the matching `TopicBufferWriter`.

    Key features:
    - Thread-safe: Uses file locks to ensure safe concurrent reads.
    - O(1) memory reading: No loading data into memory, but at the cost of disk I/O.

    """

    def __init__(self, path: pathlib.Path, topic: str) -> None:
        """Initialize a TopicBufferReader.

        Args:
            path (pathlib.Path): The base path to the topic buffer.
            topic (str): The name of the topic to read messages from.

        """
        self._topic = topic

        paths = _artifact_paths(path, self._topic)
        self._metadata_file = paths["metadata_file"]
        self._definition_file = paths["definition_file"]
        self._struct_file = paths["struct_file"]
        self._data_directory = paths["data_directory"]
        self._current_data_file = paths["current_data_file"]
        self._overflow_data_file = paths["overflow_data_file"]

        self._metadata = yaml.safe_load(self._metadata_file.read_text(encoding="utf-8"))
        self._message_definition = self._definition_file.read_text(encoding="utf-8")
        with open(self._struct_file, "rb") as f:
            self._struct = pickle.load(f)  # noqa: S301
        self._file_lock = filelock.FileLock(paths["file_lock"])

    @property
    def metadata(self) -> dict[str, Any]:
        """Metadata about the topic buffer."""
        return self._metadata

    @property
    def topic(self) -> str:
        """Topic name for the messages held by this buffer."""
        return self._topic

    @property
    def type_name(self) -> str:
        """Type name of the topic."""
        return self.metadata["type_name"]

    @property
    def definition(self) -> str:
        """Message definition for the topic."""
        return self._message_definition

    @property
    def struct(self) -> pa.StructType:
        """PyArrow StructType for the topic."""
        return self._struct

    def messages(self) -> Iterator[tuple[float, dict[str, Any]]]:
        """Read messages from the topic buffer.

        Notes:
            This method captures a snapshot of the messages in the current buffer.
            It's not idempotent: subsequent calls may yield differently as new messages are added.

        Yields:
            Iterator[tuple[float, dict[str, Any]]]: An iterator over the timestamps in seconds
                and messages in the buffer.

        """
        with tempfile.NamedTemporaryFile(
            mode="w+", encoding="utf-8", suffix=".jsonl", delete=True
        ) as combined_file:
            with self._file_lock:
                for file in [self._overflow_data_file, self._current_data_file]:
                    if file.exists():
                        with file.open(encoding="utf-8") as f:
                            shutil.copyfileobj(f, combined_file)
            combined_file.flush()

            total_bytes = 0
            combined_file.seek(0)
            for line in combined_file:
                total_bytes += len(line.encode("utf-8"))

            buffer_bytes = self.metadata.get("buffer_size_bytes", total_bytes)
            bytes_to_skip = max(0, total_bytes - buffer_bytes)

            combined_file.seek(0)
            for line in combined_file:
                if bytes_to_skip > 0:
                    bytes_to_skip -= len(line.encode("utf-8"))
                    continue
                record = json.loads(line)
                yield (record[settings.TIMESTAMP_SECONDS_COLUMN_NAME], record[self.topic])
