"""Mixin for topic image processing operators."""

import pathlib
from collections import deque
from collections.abc import Iterator

import yaml
from PIL import Image

from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import DataSource, resolve
from src.image.base import ImageDataset
from src.pipeline import base
from src.source.base import BoundedSourceFactory, SourceFactory
from src.topic.base import TopicRegistry


class TopicImageMixin:
    """Mixin for operators that work on images in a topic."""

    _factory: SourceFactory
    _registry: TopicRegistry
    _dataset: ImageDataset

    @property
    def factory(self) -> SourceFactory:
        """Return the source factory."""
        return self._factory

    @property
    def registry(self) -> TopicRegistry:
        """Return the topic registry."""
        return self._registry

    @property
    def dataset(self) -> ImageDataset:
        """Return the image dataset."""
        return self._dataset

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Implement `base.Operator.setup`."""
        ds_type = resolve(path)

        factory_module = f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}"
        registry_module = f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}"
        dataset_module = None
        match ds_type:
            case DataSource.ROS1_BAG:
                dataset_module = f"{BaseModule.IMAGE_DATASET.value}.ros1.bag"
            case DataSource.BAGEL_SINK:
                metadata_file = pathlib.Path(path) / "metadata.yaml"
                metadata = yaml.safe_load(metadata_file.read_text())
                dataset_module = metadata.get("image_dataset_module")
        if dataset_module is None:
            raise ValueError(f"{ds_type} not supported")

        self._factory = module.provide(factory_module, {"path": path, **kwargs})
        self._registry = module.provide(registry_module, {**kwargs})
        self._dataset = module.provide(dataset_module, {**kwargs})

    def to_images(
        self,
        topics: list[str] | None,
        asof_seconds: float,
        lookback: base.Lookback | None = None,
    ) -> Iterator[tuple[str, float, Image.Image]]:
        """Yield images up to `asof_seconds`.

        Thin wrapper around `ImageDataset.images` to support lookback.

        """
        start_seconds = None
        if (
            lookback
            and lookback.unit != base.Unit.FRAME
            and asof_seconds - lookback.to_seconds() >= 0
        ):
            start_seconds = asof_seconds - lookback.to_seconds()

        if isinstance(self.factory, BoundedSourceFactory) and self.dataset.use_cache:
            images = self.dataset.images(
                factory=self.factory,
                registry=self.registry,
                topics=topics,
                start_seconds=None,
                end_seconds=None,
            )
            images = iter(
                (topic, timestamp, image)
                for topic, timestamp, image in images
                if (start_seconds is None or timestamp >= start_seconds)
                and timestamp <= asof_seconds
            )
        else:
            images = self.dataset.images(
                factory=self.factory,
                registry=self.registry,
                topics=topics,
                start_seconds=start_seconds,
                end_seconds=asof_seconds,
            )

        if lookback and lookback.unit == base.Unit.FRAME:
            queue = deque(maxlen=lookback.last)
            for item in images:
                queue.append(item)
            images = iter(queue)

        yield from images
