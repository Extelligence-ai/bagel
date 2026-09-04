"""A message dataset for PostgreSQL / TimescaleDB databases.

Overrides `to_duckdb` to query the attached database directly -- the database is the
source of truth, so no Arrow-file caching is used and every query sees fresh data.
The relation matches the standard contract: a `timestamp_seconds` column plus one
struct column named after each topic.
"""

from collections.abc import Iterator
from typing import Any

import duckdb
import pyarrow as pa

from settings import settings
from src.di import module
from src.message import base
from src.query import connection
from src.source.base import SourceFactory
from src.source.postgres import PostgresDatabase, quote_identifier
from src.topic.base import TopicRegistry


class MessageDataset(base.MessageDataset):
    """A message dataset for PostgreSQL/TimescaleDB databases."""

    def __init__(self) -> None:
        """Initialize the dataset (no Arrow-file caching; the DB is authoritative)."""
        super().__init__(use_cache=False)

    def _topic_select(
        self,
        data_source: PostgresDatabase,
        topic: str,
        start_seconds: float | None,
        end_seconds: float | None,
        empty: bool = False,
    ) -> str:
        """Build the `timestamp_seconds + <topic> struct` SELECT for one table."""
        timestamp_column = quote_identifier(data_source.timestamp_column(topic))
        packed = ", ".join(
            f"{quote_identifier(name)} := {quote_identifier(name)}"
            for name, _ in data_source.columns(topic)
        )
        conditions = ["TRUE"]
        if start_seconds is not None:
            conditions.append(f"epoch({timestamp_column}) >= {start_seconds}")
        if end_seconds is not None:
            conditions.append(f"epoch({timestamp_column}) <= {end_seconds}")
        if empty:
            conditions.append("FALSE")

        return (
            f"SELECT epoch({timestamp_column})::DOUBLE "  # noqa: S608
            f'AS "{settings.TIMESTAMP_SECONDS_COLUMN_NAME}", '
            f"struct_pack({packed}) AS {quote_identifier(topic)} "
            f"FROM {data_source.relation_name(topic)} "
            f"WHERE {' AND '.join(conditions)}"
        )

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
        """Return a DuckDB relation querying the database directly."""
        data_source = factory.build()
        topics = topics or registry.available_topics(data_source)

        selects = [
            self._topic_select(data_source, topic, start_seconds, end_seconds, empty)
            for topic in topics
        ]
        # UNION ALL BY NAME lines up shared columns and fills missing structs with NULL.
        query = " UNION ALL BY NAME ".join(f"({select})" for select in selects)
        return connection().sql(f'{query} ORDER BY "{settings.TIMESTAMP_SECONDS_COLUMN_NAME}"')

    def _messages(
        self,
        data_source: PostgresDatabase,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, object]]:
        """Yield (topic, timestamp seconds, row dict) tuples from the database."""
        for topic in topics:
            relation = connection().sql(
                self._topic_select(
                    data_source, topic, start_seconds_inclusive, end_seconds_inclusive
                )
            )
            for timestamp_seconds, row in relation.fetchall():
                yield topic, float(timestamp_seconds), row

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Rows come back from DuckDB as dictionaries already."""
        return dict(message)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
