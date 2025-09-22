"""An abstract base class for logging message datasets."""

import abc

import duckdb
import pyarrow as pa

from settings import settings
from src.message.base import MessageDataset
from src.source.base import SourceFactory
from src.topic.base import TopicRegistry


class NoLoggingTopicsFoundError(Exception):
    """Raised when no logging topics are found in the data source."""


class LoggingDataset(abc.ABC):
    """An abstract base class for reading logging messages from a data source.

    This ABC provides a common contract for implementing reader of logging messages,
    e.g., INFO, WARN, ERROR messages.

    Note:
        Constructors of all subclasses must only accept primitive types (e.g.,
        str, int, bool). This ensures instances can be reliably serialized and
        recreated via dependency injection.

    """

    @abc.abstractmethod
    def to_duckdb(
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation of the logging message dataset.

        Args:
            factory (SourceFactory): The source factory for creating the data source.
            registry (TopicRegistry): The topic registry for looking up topic schemas.
            start_seconds (float | None, optional): The start time seconds (inclusive).
                If None, starts from the beginning.
            end_seconds (float | None, optional): The end time seconds (exclusive).
                If None, reads until the end.

        Returns:
            duckdb.DuckDBPyRelation: A DuckDB relation of the message dataset.

        """


class TopicBasedLoggingDataset(LoggingDataset, MessageDataset):
    """Base class for LoggingDataset where logging messages are from specific topics."""

    @property
    @abc.abstractmethod
    def type_name(self) -> str:
        """The native type name of the logging topic in the data source."""

    def to_duckdb(
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation of the logging message dataset."""
        data_source = factory.build()
        topics = [
            topic
            for topic in registry.available_topics(data_source)
            if registry.native_type_name(topic, data_source) == self.type_name
        ]
        if not topics:
            raise NoLoggingTopicsFoundError(self.type_name)
        struct = registry.struct(topics[0], data_source)
        schema = pa.schema(
            [
                pa.field(settings.TIMESTAMP_SECONDS_COLUMN_NAME, pa.float64(), nullable=False),
                pa.field("topic", pa.string(), nullable=False),
                pa.field("message", struct, nullable=False),
            ]
        )
        records = []
        for topic, timestamp, message in self._messages(
            data_source, topics, start_seconds, end_seconds
        ):
            records.append(
                {
                    settings.TIMESTAMP_SECONDS_COLUMN_NAME: timestamp,
                    "topic": topic,
                    "message": self._to_json(message, struct),
                }
            )
        table = pa.Table.from_pylist(records, schema=schema)
        return duckdb.from_arrow(table)
