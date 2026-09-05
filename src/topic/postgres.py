"""A topic registry for PostgreSQL / TimescaleDB databases.

Each user table is a topic. Schemas come straight from DuckDB's view of the attached
table (a LIMIT-0 query's Arrow schema), so types are exact with no mapping tables.
"""

import pyarrow as pa

from src.di import module
from src.query import connection
from src.source.postgres import PostgresDatabase
from src.topic import base


class TopicRegistry(base.TopicRegistry):
    """A topic registry for PostgreSQL/TimescaleDB databases."""

    def available_topics(self, data_source: PostgresDatabase) -> list[str]:
        """Return a list of available topic (table) names."""
        return [data_source.topic_of(schema, table) for schema, table in data_source.tables()]

    def bounds_topics(self, data_source: PostgresDatabase) -> list[str]:
        """Return topics (tables) with a resolvable timestamp column.

        An ordinary lookup table alongside an event table is still a valid topic
        for direct queries, but has no timestamp column to aggregate for
        whole-source bounds; skip it rather than fail the bounds query entirely.
        """
        topics = []
        for topic in self.available_topics(data_source):
            try:
                data_source.timestamp_column(topic)
            except ValueError:
                continue
            topics.append(topic)
        return topics

    def _require_topic(self, topic: str, data_source: PostgresDatabase) -> None:
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

    def native_type_name(self, topic: str, data_source: PostgresDatabase) -> str:
        """Return the native type name for the given topic."""
        self._require_topic(topic, data_source)
        return "postgres/table"

    def message_count(self, topic: str, data_source: PostgresDatabase) -> int:
        """Return the number of rows for the given topic."""
        self._require_topic(topic, data_source)
        (count,) = (
            connection()
            .execute(
                f"SELECT COUNT(*) FROM {data_source.relation_name(topic)}"  # noqa: S608
            )
            .fetchone()
        )
        return count

    def struct(self, topic: str, data_source: PostgresDatabase) -> pa.StructType:
        """Return the PyArrow StructType for the given topic (all columns)."""
        self._require_topic(topic, data_source)
        empty = (
            connection()
            .sql(
                f"SELECT * FROM {data_source.relation_name(topic)} LIMIT 0"  # noqa: S608
            )
            .arrow()
        )
        return pa.struct(empty.schema)

    def describe(self, topic: str, data_source: PostgresDatabase) -> str:
        """Return a human-readable description: the table's columns and types."""
        self._require_topic(topic, data_source)
        timestamp_column = data_source.timestamp_column(topic)
        lines = [f"table {topic} (timestamp column: {timestamp_column})"]
        lines += [f"  {name}: {type_}" for name, type_ in data_source.columns(topic)]
        return "\n".join(lines)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
