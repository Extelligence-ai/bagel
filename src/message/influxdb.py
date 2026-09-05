"""A message dataset for InfluxDB 3 databases.

Overrides `to_duckdb`: InfluxDB returns Arrow tables over Flight SQL, which register
directly into DuckDB -- no serialization step and no Arrow-file caching (the database
is the source of truth, so every query sees fresh data). The relation matches the
standard contract: a `timestamp_seconds` column plus one struct column per topic.
"""

from collections.abc import Iterator
from typing import Any

import duckdb
import pyarrow as pa

from settings import settings
from src.di import module
from src.message import base
from src.query import connection, from_arrow
from src.source.base import SourceFactory
from src.source.influxdb import InfluxDatabase
from src.source.postgres import quote_identifier
from src.topic.base import TopicRegistry


class MessageDataset(base.MessageDataset):
    """A message dataset for InfluxDB 3 databases."""

    def __init__(self) -> None:
        """Initialize the dataset (no Arrow-file caching; the DB is authoritative)."""
        super().__init__(use_cache=False)

    def _topic_arrow(
        self,
        data_source: InfluxDatabase,
        topic: str,
        start_seconds: float | None,
        end_seconds: float | None,
    ) -> pa.Table:
        """Fetch one measurement as an Arrow table, filtered to the time range."""
        conditions = ["TRUE"]
        if start_seconds is not None:
            conditions.append(f"time >= to_timestamp({start_seconds})")
        if end_seconds is not None:
            conditions.append(f"time <= to_timestamp({end_seconds})")
        return data_source.client.query(
            f'SELECT * FROM "{topic}" WHERE {" AND ".join(conditions)} ORDER BY time'  # noqa: S608
        )

    def _topic_relation(
        self,
        data_source: InfluxDatabase,
        topic: str,
        start_seconds: float | None,
        end_seconds: float | None,
        empty: bool = False,
    ) -> duckdb.DuckDBPyRelation:
        """Build the `timestamp_seconds + <topic> struct` relation for one measurement."""
        if empty:
            arrow_table = data_source.client.query(f'SELECT * FROM "{topic}" LIMIT 0')  # noqa: S608
        else:
            arrow_table = self._topic_arrow(data_source, topic, start_seconds, end_seconds)

        packed = ", ".join(
            f"{quote_identifier(name)} := {quote_identifier(name)}"
            for name in arrow_table.schema.names
        )
        relation = from_arrow(arrow_table)
        return relation.project(
            f'epoch("time")::DOUBLE AS "{settings.TIMESTAMP_SECONDS_COLUMN_NAME}", '
            f"struct_pack({packed}) AS {quote_identifier(topic)}"
        )

    def bounds(self, factory: SourceFactory, registry: TopicRegistry) -> tuple[float, float]:
        """Aggregate MIN/MAX time per measurement instead of downloading every row.

        Unlike `to_duckdb()` (which downloads and struct-packs every measurement's
        full rows to build a relation), this only needs each measurement's min/max
        `time`, computed by InfluxDB itself. `epoch()` on the Arrow result matches
        the seconds conversion `_topic_relation` uses for the full relation.
        """
        data_source = factory.build()
        lo: float | None = None
        hi: float | None = None
        for topic in registry.available_topics(data_source):
            arrow_table = data_source.client.query(
                f'SELECT MIN(time) AS lo, MAX(time) AS hi FROM "{topic}"'  # noqa: S608
            )
            if arrow_table.num_rows == 0:
                continue
            row = from_arrow(arrow_table).aggregate("epoch(min(lo)), epoch(max(hi))").fetchone()
            if row is None or row[0] is None:
                continue
            lo = float(row[0]) if lo is None else min(lo, float(row[0]))
            hi = float(row[1]) if hi is None else max(hi, float(row[1]))
        return (lo, hi) if lo is not None and hi is not None else (0.0, 0.0)

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
        """Return a DuckDB relation over the measurements' Arrow data."""
        data_source = factory.build()
        topics = topics or registry.available_topics(data_source)

        combined = None
        for topic in topics:
            relation = self._topic_relation(data_source, topic, start_seconds, end_seconds, empty)
            # union() on projections with differing struct columns lines up by position,
            # so register each relation and combine with UNION ALL BY NAME in SQL.
            alias = f"influx_{abs(hash((data_source.database, topic))) % 10**8}"
            connection().register(alias, relation.arrow())
            select = f'SELECT * FROM "{alias}"'  # noqa: S608
            combined = select if combined is None else f"{combined} UNION ALL BY NAME {select}"

        return connection().sql(f'{combined} ORDER BY "{settings.TIMESTAMP_SECONDS_COLUMN_NAME}"')

    def _messages(
        self,
        data_source: InfluxDatabase,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, object]]:
        """Yield (topic, timestamp seconds, row dict) tuples from the database."""
        for topic in topics:
            relation = self._topic_relation(
                data_source, topic, start_seconds_inclusive, end_seconds_inclusive
            )
            for timestamp_seconds, row in relation.fetchall():
                yield topic, float(timestamp_seconds), row

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        """Rows come back from DuckDB as dictionaries already."""
        return dict(message)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
