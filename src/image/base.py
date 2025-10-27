"""An abstract base class for image datasets."""

import abc
from collections.abc import Iterator

from PIL import Image

from src.source.base import SourceFactory
from src.topic.base import TopicRegistry


class NotImageTopicError(Exception):
    """Raised when a topic is not an image topic."""


class ImageDataset(abc.ABC):
    """An abstract base class for image datasets.

    This ABC provides a common contract for implementing extractor of images,
    e.g., camera images.

    Note:
        Constructors of all subclasses must only accept primitive types (e.g.,
        str, int, bool). This ensures instances can be reliably serialized and
        recreated via dependency injection.

    """

    def __init__(self, use_cache: bool = True) -> None:
        """Initialize the ImageDataset.

        Args:
            use_cache (bool, optional): Whether to use cached image data on disk. Some data formats
                may not support caching. In such cases, use_cache should be set to False.

        """
        self._use_cache = use_cache

    @property
    def use_cache(self) -> bool:
        """Return whether to use cached image data on disk if available."""
        return self._use_cache

    @property
    @abc.abstractmethod
    def image_type_name(self) -> str:
        """The native type name of image topics."""

    @abc.abstractmethod
    def _images(
        self,
        data_source: object,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, Image.Image]]:
        """Return an iterator over images from the data source.

        Args:
            data_source (object): The data source to extract images from.
            topics (list[str]): The list of topics to extract.
            start_seconds_inclusive (float | None): The start time seconds (inclusive).
                If None, starts from the beginning.
            end_seconds_inclusive (float | None): The end time seconds (inclusive).
                If None, reads until the end.

        Yields:
            Iterator[tuple[str, float, Image.Image]]: Yielding tuples of topic,
                timestamp in seconds, and the PIL.Image object.

        """

    def images(  # noqa: D417
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        topics: list[str] | None = None,
        start_seconds: float | None = None,
        end_seconds: float | None = None,
    ) -> Iterator[tuple[str, float, Image.Image]]:
        """Return an iterator over images from the data source.

        Args:
            factory (SourceFactory): The source factory for creating the data source.
            registry (TopicRegistry): The topic registry for looking up topic schemas.
            topics (list[str]): The list of topics to extract.
            start_seconds_inclusive (float | None): The start time seconds (inclusive).
                If None, starts from the beginning.
            end_seconds_inclusive (float | None): The end time seconds (inclusive).
                If None, reads until the end.

        Raises:
            NotImageTopicError: Raised when a topic is not an image topic.

        Yields:
            Iterator[tuple[str, float, Image.Image]]: Yielding tuples of topic,
                timestamp in seconds, and the PIL.Image object.

        """
        data_source = factory.build()
        image_topics = [
            topic
            for topic in registry.available_topics(data_source)
            if registry.native_type_name(topic, data_source) == self.image_type_name
        ]
        none_image_topics = [topic for topic in topics or [] if topic not in image_topics]
        if none_image_topics:
            raise NotImageTopicError(none_image_topics)
        yield from self._images(data_source, topics or image_topics, start_seconds, end_seconds)
