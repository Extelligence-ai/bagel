"""A topic registry for Bagel TopicSink."""

import pyarrow as pa

from src.di import module
from src.sink import reader
from src.topic import base


class TopicRegistry(base.TopicRegistry):
    """A topic registry for Bagel TopicSink.

    Notes:
        None of the methods in this class is idempotent. As topics can be subscribed
        or unsubscribed at any time, subsequent calls to these methods may yield
        different results.

    """

    def available_topics(self, data_source: reader.TopicSinkReader) -> list[str]:
        """Return a list of available topic names."""
        return sorted(data_source.subscribed_topics())

    def native_type_name(self, topic: str, data_source: reader.TopicSinkReader) -> str:
        """Return the native type name for the given topic."""
        return data_source.reader(topic).type_name

    def message_count(self, topic: str, data_source: reader.TopicSinkReader) -> None:
        """Return the number of messages for the given topic."""
        return None  # unbounded

    def struct(self, topic: str, data_source: reader.TopicSinkReader) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        return data_source.reader(topic).struct

    def describe(self, topic: str, data_source: reader.TopicSinkReader) -> str:
        """Return a human-readable description of the given topic."""
        return data_source.reader(topic).definition


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
