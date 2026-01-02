"""Provide a reader for topic messages from a sink directory."""

import pathlib
from typing import Any

import yaml

from src.sink import base, buffer


class TopicSinkReader:
    """A reader interface for consuming messages from a sink directory.

    The `TopicSinkReader` provides a high-level entry point for inspecting
    and consuming topics persisted in a sink. A sink represents a collection
    of topic buffers.

    Notes:
        - The set of subscribed topics is not static; repeated calls to
          `subscribed_topics()` may reflect new or removed topics.
        - Each `TopicBufferReader` exposes access to the underlying
          file-backed buffer of a single topic.

    """

    def __init__(self, path: str) -> None:
        """Initialize a TopicSinkReader.

        Args:
            path (str): Path to the sink directory.

        """
        self._path = pathlib.Path(path)
        self._metadata = yaml.safe_load((self._path / "metadata.yaml").read_text(encoding="utf-8"))

    @property
    def path(self) -> pathlib.Path:
        """Path to the sink directory."""
        return self._path

    @property
    def metadata(self) -> dict[str, Any]:
        """Metadata about the sink."""
        return self._metadata

    def subscribed_topics(self) -> list[str]:
        """Return the list of topics that are currently subscribed.

        Notes:
            Not idempotent: subsequent calls may yield different results as topics are subscribed.

        """
        return [
            yaml.safe_load(file.read_text(encoding="utf-8"))["topic"]
            for file in (self._path / "metadata").glob("*.yaml")
        ]

    def reader(self, topic: str) -> buffer.TopicBufferReader:
        """Return a TopicBufferReader for the specified topic.

        Notes:
            Not idempotent: subsequent calls may yield different results as topics are subscribed.

        """
        if topic not in self.subscribed_topics():
            raise base.TopicNotFoundError(topic)
        return buffer.TopicBufferReader(self._path, topic)
