"""Abstract base class for topic sinks."""

import abc
import pathlib
import shutil
import uuid
import weakref
from typing import Any

import pyarrow as pa
import yaml

from src import artifacts
from src.sink.buffer import TopicBufferWriter

# A global registry to hold singleton instances of TopicSink instances.
_global_sink_singletons: dict[tuple[str, int], "TopicSink"] = {}  # (host, port) -> instance


class TopicNotFoundError(Exception):
    """Raised when topic is not found."""


class TopicSink(abc.ABC):
    """Abstract base class for topic sinks.

    A `TopicSink` manages a live connection to a message stream, such as a
    rosbridge or telemetry server. It provides topic discovery,
    subscription management, and local disk buffers backed by `TopicBufferWriter`.

    Each sink instance is uniquely identified by its `(host, port)` and managed as a
    singleton, ensuring that at most one active connection exists for the same
    endpoint.

    Note:
        Constructors of all subclasses must only accept primitive types (e.g.,
        str, int, bool). This ensures instances can be reliably serialized and
        recreated via dependency injection.

    """

    def __new__(cls, host: str, port: str, *args: object, **kwargs: object) -> "TopicSink":
        """Implement singleton pattern to ensure only one instance per (host, port)."""
        if (host, port) not in _global_sink_singletons:
            _global_sink_singletons[(host, port)] = super().__new__(cls)
        return _global_sink_singletons[(host, port)]

    def __init__(self, host: str, port: str, overwrite: bool) -> None:
        """Initialize the topic sink.

        Args:
            host (str): Hostname of the live data stream.
            port (str): Port number of the live data stream.
            overwrite (bool): If True, remove any existing sink directory.

        """
        weakref.finalize(self, self.close)  # ensure clean-up on deletion
        self._connect()

        # Assign attributes
        self._host = host
        self._port = port
        self._all_topics = self._available_topics()

        # Prepare the sink local directory
        self._directory = artifacts.sink_directory(
            str(uuid.uuid5(uuid.NAMESPACE_OID, "_".join([self._host, str(self._port)])))
        )
        if self._directory.exists() and overwrite:
            shutil.rmtree(self._directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        metadata_file = self._directory / "metadata.yaml"
        if not metadata_file.exists():
            with open(metadata_file, "w", encoding="utf-8") as f:
                f.write(yaml.safe_dump(self.metadata))

        # Initialize topic buffers
        self._buffers: dict[str, TopicBufferWriter] = {}  # topic -> buffer

    @abc.abstractmethod
    def _connect(self) -> None:
        """Establish a live connection to the data stream.

        Notes:
            Must be idempotent: no-op if already connected.

        """

    @abc.abstractmethod
    def _disconnect(self) -> None:
        """Terminate the connection.

        Notes:
            Must be idempotent: no-op if already disconnected.
            After disconnect, the sink must not be reused.

        """

    @abc.abstractmethod
    def _available_topics(self) -> list[str]:
        """Return the list of topics can be subscribed to."""

    @abc.abstractmethod
    def _type_name(self, topic: str) -> str:
        """Return the type name of a topic."""

    @abc.abstractmethod
    def _definition(self, topic: str) -> str:
        """Return the message definition of a topic."""

    @abc.abstractmethod
    def _struct(self, topic: str) -> pa.StructType:
        """Return the PyArrow StructType of a topic."""

    @abc.abstractmethod
    def _subscribe(self, writer: TopicBufferWriter) -> None:
        """Begin sinking topic messages into the provided buffer writer."""

    @abc.abstractmethod
    def _unsubscribe(self, writer: TopicBufferWriter) -> None:
        """Stop sinking topic messages."""

    @property
    def host(self) -> str:
        """Hostname of the live data stream."""
        return self._host

    @property
    def port(self) -> int:
        """Port number of the live data stream."""
        return self._port

    @property
    def available_topics(self) -> list[str]:
        """Topics currently available for subscription."""
        return self._all_topics

    @property
    def directory(self) -> pathlib.Path:
        """Path to the local sink directory."""
        return self._directory

    @property
    def metadata(self) -> dict[str, Any]:
        """Metadata about the topic sink."""
        return {
            "host": self._host,
            "port": self._port,
            "available_topics": self.available_topics,
            "magic": "BAGEL_SINK",  # Magic keyword to identify Bagel sink directories
        }

    def start(self, topics: list[str] | None = None) -> None:
        """Subscribe to a list of topics.

        Args:
            topics (list[str] | None, optional): List of topics to subscribe to. If None,
                subscribes to all available topics.

        Raises:
            TopicNotFoundError: If any requested topic is unavailable.

        """
        topics = topics or self.available_topics
        missing_topics = [t for t in topics if t not in self.available_topics]
        if missing_topics:
            raise TopicNotFoundError(missing_topics)
        for topic in topics:
            if topic not in self._buffers:
                self._buffers[topic] = TopicBufferWriter(
                    self.directory,
                    topic,
                    self._type_name(topic),
                    self._definition(topic),
                    self._struct(topic),
                )
            self._subscribe(self._buffers[topic])

    def pause(self, topics: list[str] | None = None) -> None:
        """Pause subscriptions for topics.

        Notes:
            Messages received while paused are dropped until `start()` is called again.

        Args:
            topics (list[str] | None, optional): The list of topics to pause. If None,
                pauses all subscribed topics.

        Raises:
            TopicNotFoundError: If any requested topic is not currently subscribed.

        """
        topics = topics or list(self._buffers.keys())
        missing_topics = [t for t in topics if t not in self._buffers]
        if missing_topics:
            raise TopicNotFoundError(missing_topics)
        for topic in topics:
            self._unsubscribe(self._buffers[topic])

    def close(self) -> None:
        """Disconnect and release all resources.

        Notes:
            After closing, the sink must not be reused.

        """
        try:
            self.pause()
            self._disconnect()
        finally:
            del _global_sink_singletons[(self.host, self.port)]

    def __enter__(self) -> "TopicSink":  # noqa: D105
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # noqa: ANN001, D105
        self.close()
