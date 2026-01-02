"""A logging message dataset for PX4 ULogs."""

import duckdb
import pyarrow as pa

from settings import settings
from src.di import module
from src.logging import base
from src.source.base import SourceFactory
from src.topic.base import TopicRegistry

MICROSECOND = 1
MILLISECOND = 1000 * MICROSECOND
SECOND = 1000 * MILLISECOND


class LoggingDataset(base.LoggingDataset):
    """A logging message dataset for PX4 ULogs."""

    def to_duckdb(
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation of the logging message dataset."""
        schema = pa.schema(
            [
                pa.field(settings.TIMESTAMP_SECONDS_COLUMN_NAME, pa.float64(), nullable=False),
                pa.field(
                    "message",
                    pa.struct(
                        [
                            pa.field("log_level", pa.string(), nullable=False),
                            pa.field("message", pa.string(), nullable=False),
                        ]
                    ),
                ),
            ]
        )
        records = []
        for message in factory.build().logged_messages:
            timestamp_seconds = message.timestamp / SECOND
            if start_seconds is not None and timestamp_seconds < start_seconds:
                continue
            if end_seconds is not None and timestamp_seconds >= end_seconds:
                continue
            records.append(
                {
                    settings.TIMESTAMP_SECONDS_COLUMN_NAME: message.timestamp / SECOND,
                    "message": {
                        "log_level": message.log_level_str(),
                        "message": message.message,
                    },
                }
            )
        table = pa.Table.from_pylist(records, schema=schema)
        return duckdb.from_arrow(table)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
