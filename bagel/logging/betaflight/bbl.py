"""A logging message dataset for Betaflight Blackbox logs."""

import duckdb

from bagel.di import module
from bagel.logging import base
from bagel.source.base import SourceFactory
from bagel.topic.base import TopicRegistry


class LoggingMessagesNotSupportedError(Exception):
    """Raised when Betaflight Blackbox logging is not supported."""


class LoggingDataset(base.LoggingDataset):
    """A logging message dataset for Betaflight Blackbox logs."""

    def to_duckdb(
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation of the logging message dataset."""
        raise LoggingMessagesNotSupportedError()


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
