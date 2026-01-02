"""Mixin for topic message processing operators."""

import duckdb

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import resolve
from src.message.base import MessageDataset
from src.pipeline import base
from src.source.base import BoundedSourceFactory, SourceFactory
from src.topic.base import TopicRegistry


class TopicMessageMixin:
    """Mixin for operators that work on messages in a topic."""

    _factory: SourceFactory
    _registry: TopicRegistry
    _dataset: MessageDataset

    @property
    def factory(self) -> SourceFactory:
        """Return the source factory."""
        return self._factory

    @property
    def registry(self) -> TopicRegistry:
        """Return the topic registry."""
        return self._registry

    @property
    def dataset(self) -> MessageDataset:
        """Return the message dataset."""
        return self._dataset

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Implement `base.Operator.setup`."""
        ds_type = resolve(path)

        factory_module = f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}"
        registry_module = f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}"
        dataset_module = f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}"

        self._factory = module.provide(factory_module, {"path": path, **kwargs})
        self._registry = module.provide(registry_module, {**kwargs})
        self._dataset = module.provide(dataset_module, {**kwargs})

    def to_duckdb(
        self,
        topics: list[str] | None,
        asof_seconds: float,
        lookback: base.Lookback | None = None,
        ffill: bool = False,
    ) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation of topic messages up to `asof_seconds`.

        Thin wrapper around `MessageDataset.to_duckdb` to support lookback.

        """
        start_seconds = None
        if (
            lookback
            and lookback.unit != base.Unit.FRAME
            and asof_seconds - lookback.to_seconds() >= 0
        ):
            start_seconds = asof_seconds - lookback.to_seconds()

        if isinstance(self.factory, BoundedSourceFactory) and self.dataset.use_cache:
            relation = self.dataset.to_duckdb(
                factory=self.factory,
                registry=self.registry,
                topics=topics,
                start_seconds=None,
                end_seconds=None,
                ffill=ffill,
                empty=False,
            )
            if start_seconds is not None:
                relation = relation.filter(
                    f"{settings.TIMESTAMP_SECONDS_COLUMN_NAME} >= {start_seconds}"
                )
            relation = relation.filter(
                f"{settings.TIMESTAMP_SECONDS_COLUMN_NAME} <= {asof_seconds}"
            )
        else:
            relation = self.dataset.to_duckdb(
                factory=self.factory,
                registry=self.registry,
                topics=topics,
                start_seconds=start_seconds,
                end_seconds=asof_seconds,
                ffill=ffill,
                empty=False,
            )

        if lookback and lookback.unit == base.Unit.FRAME:
            relation = (
                relation.order(f"{settings.TIMESTAMP_SECONDS_COLUMN_NAME} DESC")
                .limit(lookback.last)
                .order(f"{settings.TIMESTAMP_SECONDS_COLUMN_NAME} ASC")
            )

        return relation
