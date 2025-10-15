"""Abstract base class for topic sinks."""

import abc
import logging
import pathlib
import uuid
import weakref
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import yaml

from settings import settings
from src import artifacts
from src.pipeline.base import OnceAtEnd, Pipeline
from src.sink.buffer import TopicBufferWriter

# A global registry to hold singleton instances of TopicSink instances.
_global_sink_singletons: dict[tuple[str, int], "TopicSink"] = {}  # (host, port) -> instance


class TopicNotFoundError(Exception):
    """Raised when topic is not found."""


class TopicAlreadySubscribedError(Exception):
    """Raised when topic is already subscribed."""


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

    _is_singleton_initialized: bool

    def __new__(cls, host: str, port: str, *args: object, **kwargs: object) -> "TopicSink":
        """Implement singleton pattern to ensure only one instance per (host, port)."""
        if (host, port) not in _global_sink_singletons:
            instance = super().__new__(cls)
            instance._is_singleton_initialized = False
            _global_sink_singletons[(host, port)] = instance
        return _global_sink_singletons[(host, port)]

    def __init__(self, host: str, port: str) -> None:
        """Initialize the topic sink.

        Args:
            host (str): Hostname of the live data stream.
            port (str): Port number of the live data stream.

        """
        if self._is_singleton_initialized:
            return

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
        self._directory.mkdir(parents=True, exist_ok=True)
        metadata_file = self._directory / "metadata.yaml"
        if not metadata_file.exists():
            with open(metadata_file, "w", encoding="utf-8") as f:
                f.write(yaml.safe_dump(self.metadata))

        # Initialize topic buffers
        self._buffers: dict[str, TopicBufferWriter] = {}  # topic -> buffer

        self._is_singleton_initialized = True

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

    def subscribe(
        self,
        topic: str,
        pipeline: Pipeline | None = None,
        overwrite: bool = False,
        buffer_size_bytes: int | None = settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES,
        extract_timestamp: Callable[[dict[str, Any]], float] | None = None,
    ) -> None:
        """Subscribe to a topic.

        A topic can only be subscribed once, unless using `overwrite=True` to re-subscribe.

        Args:
            topic (str): The topic to subscribe to.
            pipeline (Pipeline | None, optional): An callback pipeline to execute on
                incoming messages.
            overwrite (bool, optional): If True, re-subscribe to the topic and overwrite any
                existing topic buffers.
            buffer_size_bytes (int | None, optional): The maximum buffer size in bytes before
                evicting old messages. If None, the buffer size is unbounded.
            extract_timestamp (Callable[[dict[str, Any]], float] | None, optional):
                A function to extract a timestamp in seconds from a message. If None,
                the current system time is used.

        Raises:
            TopicNotFoundError: If any requested topic is unavailable.

        """
        if topic not in self.available_topics:
            raise TopicNotFoundError(topic)

        if topic in self._buffers and not overwrite:
            raise TopicAlreadySubscribedError(topic)

        self._buffers[topic] = TopicBufferWriter(
            self.directory,
            topic,
            self._type_name(topic),
            self._definition(topic),
            self._struct(topic),
            buffer_size_bytes=buffer_size_bytes,
            overwrite=overwrite,
            pipeline=pipeline,
            extract_timestamp=extract_timestamp,
        )

        if topic in self._buffers and overwrite:
            self._unsubscribe(self._buffers[topic])

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
        """Disconnect from the data stream, run any pending pipelines, and release all resources.

        Notes:
            After closing, the sink must not be reused.

        """
        try:
            self.pause()
            self._disconnect()
        finally:
            del _global_sink_singletons[(self.host, self.port)]

            while self._buffers:
                topic, writer = self._buffers.popitem()
                if writer.pipeline is not None and isinstance(
                    writer.pipeline.cadence.when, OnceAtEnd
                ):
                    if writer.last_timestamp_seconds is None:
                        logging.info(
                            "No messages received on topic '%s', skipping pipeline '%s'",
                            topic,
                            writer.pipeline.name,
                        )
                    elif writer.last_run_at == writer.last_timestamp_seconds:
                        logging.info(
                            "Pipeline '%s' already executed on topic '%s' at the end, skipping",
                            topic,
                            writer.pipeline.name,
                        )
                    else:
                        writer.pipeline.run_at(writer.last_timestamp_seconds)
                if writer.pipeline is not None:
                    logging.info("Pipeline '%s' completed.", writer.pipeline.name)

    def __enter__(self) -> "TopicSink":  # noqa: D105
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # noqa: ANN001, D105
        self.close()
