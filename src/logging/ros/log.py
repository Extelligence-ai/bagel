"""A logging message dataset for plain-text ROS log files.

Unlike the bag-backed logging datasets, this reads the log files ROS dumps to
disk (e.g., ~/.ros/log) directly -- no bag needs to be opened.
"""

import duckdb
import pyarrow as pa

from settings import settings
from src.di import module
from src.logging import base
from src.query import from_arrow
from src.source.ros.log import SourceFactory
from src.topic.base import TopicRegistry

SCHEMA = pa.schema(
    [
        pa.field(settings.TIMESTAMP_SECONDS_COLUMN_NAME, pa.float64(), nullable=False),
        pa.field("topic", pa.string(), nullable=False),
        pa.field(
            "message",
            pa.struct(
                [
                    pa.field("level", pa.string(), nullable=False),
                    pa.field("message", pa.string(), nullable=False),
                    pa.field("file", pa.string(), nullable=False),
                ]
            ),
            nullable=False,
        ),
    ]
)


class LoggingDataset(base.LoggingDataset):
    """A logging message dataset for plain-text ROS log files."""

    def to_duckdb(
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation of the logging message dataset."""
        records = []
        for record in factory.build().records:
            if start_seconds is not None and record.timestamp_seconds < start_seconds:
                continue
            if end_seconds is not None and record.timestamp_seconds > end_seconds:
                continue
            records.append(
                {
                    settings.TIMESTAMP_SECONDS_COLUMN_NAME: record.timestamp_seconds,
                    "topic": record.node,
                    "message": {
                        "level": record.level,
                        "message": record.message,
                        "file": record.file,
                    },
                }
            )
        table = pa.Table.from_pylist(records, schema=SCHEMA)
        return from_arrow(table)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
