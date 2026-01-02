"""A image dataset reading image messages from a Bagel sink."""

import abc
from collections.abc import Iterator
from typing import Any

from PIL import Image

from src.image import base
from src.message.bagel import sink
from src.sink import reader


class ImageDataset(base.ImageDataset):
    """A image dataset reading image messages from a Bagel sink.

    Notes:
        This is **only** a base class. For specific formats (e.g., ROS2), subclass still need
        to be implemented.

    """

    def __init__(self, use_cache: bool = True) -> None:
        """Initialize the image dataset.

        Args:
            use_cache (bool, optional): Whether to use cached Apache Arrow files if available.

        """
        super().__init__(use_cache)

    @abc.abstractmethod
    def _to_image(self, msg: dict[str, Any]) -> Image.Image:
        """Cast a message dictionary into a PIL.Image object."""

    def _images(
        self,
        data_source: reader.TopicSinkReader,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, Image.Image]]:
        """Return an iterator over images from the data source."""
        dataset = sink.MessageDataset(self._use_cache)
        for topic, timestamp_seconds, msg in dataset._messages(
            data_source, topics, start_seconds_inclusive, end_seconds_inclusive
        ):
            yield topic, timestamp_seconds, self._to_image(msg)
