"""Construct the same source adapters for tools, cadence discovery, and operators."""

from dataclasses import dataclass
from typing import Any, cast

import duckdb

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import resolve
from src.message.base import MessageDataset
from src.source.base import BoundedSourceFactory, SourceFactory
from src.topic.base import TopicRegistry


@dataclass
class SourceContext:
    """The adapters interpreting one source with one set of decoding options."""

    factory: SourceFactory
    registry: TopicRegistry
    dataset: MessageDataset

    @classmethod
    def build(cls, path: str, args: dict[str, Any] | None = None) -> "SourceContext":
        """Construct adapters, preserving the explicit source path over any args path."""
        kind = resolve(path).value
        options = args or {}
        return cls(
            cast(
                SourceFactory,
                module.provide(
                    f"{BaseModule.SOURCE_FACTORY.value}.{kind}", {**options, "path": path}
                ),
            ),
            cast(
                TopicRegistry, module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{kind}", options)
            ),
            cast(
                MessageDataset,
                module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{kind}", options),
            ),
        )

    def bounds(self) -> tuple[float, float]:
        """Return whole-source bounds, not just the event topic's observed span."""
        if isinstance(self.factory, BoundedSourceFactory):
            return self.factory.start_seconds, self.factory.end_seconds
        relation = self.dataset.to_duckdb(self.factory, self.registry)
        return relation_bounds(relation)


def relation_bounds(relation: duckdb.DuckDBPyRelation) -> tuple[float, float]:
    """Aggregate a relation's timestamp bounds without materializing its rows."""
    timestamp = settings.TIMESTAMP_SECONDS_COLUMN_NAME
    row = relation.aggregate(f'min("{timestamp}"), max("{timestamp}")').fetchone()
    return (float(row[0]), float(row[1])) if row and row[0] is not None else (0.0, 0.0)
