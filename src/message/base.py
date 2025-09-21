"""An abstract base class for topic message datasets."""

import abc
from collections.abc import Iterator
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
from pydantic import BaseModel, ConfigDict

from settings import settings
from src import artifacts
from src.source.base import SourceFactory
from src.topic.base import TopicRegistry


class AccessPath(BaseModel):
    """Represents a field access path in a message dataset."""

    path: list[str]
    pa_type: pa.DataType

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def duckdb_path(self) -> str:
        """Return the DuckDB-compatible field access path using dot notation."""
        return ".".join([f'"{p}"' for p in self.path])

    @property
    def duckdb_type(self) -> str:
        """Return the DuckDB-compatible type name."""
        mapping = {
            pa.int8(): "TINYINT",
            pa.int16(): "SMALLINT",
            pa.int32(): "INTEGER",
            pa.int64(): "BIGINT",
            pa.uint8(): "UTINYINT",
            pa.uint16(): "USMALLINT",
            pa.uint32(): "UINTEGER",
            pa.uint64(): "UBIGINT",
            pa.float32(): "FLOAT",
            pa.float64(): "DOUBLE",
            pa.bool_(): "BOOLEAN",
            pa.string(): "VARCHAR",
            pa.large_string(): "VARCHAR",
            pa.binary(): "BLOB",
            pa.large_binary(): "BLOB",
        }
        return mapping[self.pa_type]


class MessageDataset(abc.ABC):
    """An abstract base class for topic message datasets."""

    def __init__(self, use_cache: bool = True) -> None:
        """Initialize the MessageDataset.

        Args:
            use_cache (bool, optional): Whether to use cached Apache Arrow files if available.

        """
        self._use_cache = use_cache

    @abc.abstractmethod
    def _messages(
        self,
        data_source: object,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_exclusive: float | None,
    ) -> Iterator[str, float, object]:
        """Return an iterator over messages from the data source.

        Args:
            data_source (object): The data source to read messages from.
            topics (list[str]): The list of topics to read.
            start_seconds_inclusive (float | None): The start time seconds (inclusive).
                If None, starts from the beginning.
            end_seconds_exclusive (float | None): The end time seconds (exclusive).
                If None, reads until the end.

        Yields:
            Iterator[str, float, object]: Yielding tuples of topic name, timestamp in seconds,
                and the message object.

        """

    @abc.abstractmethod
    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Convert a message to a JSON-serializable dictionary.

        Args:
            message (object): The message object to convert.
            struct (pa.StructType): The schema of the message.

        Returns:
            dict[str, Any]: The JSON-serializable dictionary of the message.

        """

    def to_duckdb(  # noqa: PLR0913
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        topics: list[str] | None = None,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
        ffill: bool = False,
        empty: bool = False,
    ) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation of the message dataset for the given topics and time range.

        Args:
            factory (SourceFactory): The source factory for creating the data source.
            registry (TopicRegistry): The topic registry for looking up topic schemas.
            topics (list[str] | None, optional): The list of topics to include in the dataset.
                If None, all available topics will be included.
            start_seconds (float | None, optional): The start time seconds (inclusive).
                If None, starts from the beginning.
            end_seconds (float | None, optional): The end time seconds (exclusive).
                If None, reads until the end.
            ffill (bool, optional): Whether to forward-fill missing values, i.e., use the
                last known value for a topic if a message is missing at a timestamp.
            empty (bool, optional): Whether to return an empty DuckDB relation with no messages.
                This is useful for LLMs to understand the DuckDB schema.

        Returns:
            duckdb.DuckDBPyRelation: A DuckDB relation of the message dataset.

        """
        if empty:
            topics = topics or registry.available_topics(factory.build())
            schema = self._schema(factory, registry, topics)
            return duckdb.from_arrow(schema.empty_table())

        seeds = [*(topics or [str(None)]), str(start_seconds), str(end_seconds), str(ffill)]
        arrow_file = artifacts.arrow_file(factory.uuid, seeds, "topics")
        if arrow_file.exists() and self._use_cache:
            dataset = ds.dataset(arrow_file, format="arrow")
            return duckdb.from_arrow(dataset)
        arrow_file.unlink(missing_ok=True)
        arrow_file.parent.mkdir(parents=True, exist_ok=True)

        data_source = factory.build()
        topics = topics or registry.available_topics(data_source)
        messages = self._messages(data_source, topics, start_seconds, end_seconds)
        schema = self._schema(factory, registry, topics)

        try:
            with (
                pa.OSFile(str(arrow_file), "wb") as sink,
                pa.RecordBatchFileWriter(sink, schema=schema) as writer,
            ):
                for record_batch in self._record_batches(messages, schema, ffill):
                    writer.write_batch(record_batch)
            dataset = ds.dataset(arrow_file, format="arrow")
            return duckdb.from_arrow(dataset)
        except Exception as e:
            arrow_file.unlink(missing_ok=True)
            raise e

    def _schema(
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        topics: list[str],
    ) -> pa.Schema:
        """Return the PyArrow Schema of the message dataset for the given topics.

        Args:
            factory (SourceFactory): The source factory for creating the data source.
            registry (TopicRegistry): The topic registry for looking up topic schemas.
            topics (list[str]): The list of topics to include in the schema.

        Returns:
            pa.Schema: The PyArrow Schema of the message dataset.

        """
        data_source = factory.build()
        fields = [pa.field(settings.TIMESTAMP_SECONDS_COLUMN_NAME, pa.float64(), nullable=False)]
        for topic in topics:
            struct = registry.struct(topic, data_source)
            fields.append(pa.field(topic, struct, nullable=True))
        return pa.schema(fields)

    def _record_batches(
        self, messages: Iterator[str, float, object], schema: pa.Schema, ffill: bool
    ) -> Iterator[pa.RecordBatch]:
        """Return an iterator over the record batches for the given messages.

        Args:
            messages (Iterator[str, float, object]): An iterator over the messages. Each item
                is a tuple of (topic, timestamp_seconds, message).
            schema (pa.Schema): The PyArrow Schema of the message dataset.
            ffill (bool): Whether to forward-fill missing values.

        Yields:
            Iterator[pa.RecordBatch]: An iterator over the record batches.

        """
        batch_size = settings.MIN_ARROW_RECORD_BATCH_SIZE_COUNT
        batch = {column: [] for column in schema.names}
        record = {column: None for column in schema.names}

        for topic, timestamp, message in messages:
            if not ffill:
                record = {column: None for column in schema.names}
            record[settings.TIMESTAMP_SECONDS_COLUMN_NAME] = timestamp
            record[topic] = self._to_json(message, schema.field(topic).type)
            for column, value in record.items():
                batch[column].append(value)
            if len(batch[settings.TIMESTAMP_SECONDS_COLUMN_NAME]) >= batch_size:
                record_batch = pa.RecordBatch.from_pydict(batch, schema=schema)
                estimate = int(
                    (settings.ARROW_RECORD_BATCH_SIZE_BYTES / record_batch.nbytes)
                    * record_batch.num_rows
                )
                batch_size = max(estimate, settings.MIN_ARROW_RECORD_BATCH_SIZE_COUNT)
                batch = {column: [] for column in schema.names}
                yield record_batch

        if batch[settings.TIMESTAMP_SECONDS_COLUMN_NAME]:
            yield pa.RecordBatch.from_pydict(batch, schema=schema)
