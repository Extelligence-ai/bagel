"""Provide a Bagel TopicSink data source factory."""

from typing import Any

from src.di import module
from src.sink import reader
from src.source import base, errors


class TopicNotSubscribedError(Exception):
    """Raised when a topic is not subscribed in the source stream."""


class SourceFactory(base.FileBasedSourceFactory):
    """A data source factory for reading from a Bagel TopicSink directory."""

    def __init__(self, path: str) -> None:
        """Initialize a Bagel TopicSink data source factory.

        Args:
            path (str): Path to the sink directory.

        """
        self._metadata = reader.TopicSinkReader(path=path).metadata
        super().__init__(path=path)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the topic sink."""
        return {
            **self._file_based_metadata,
            **self._metadata,
        }

    def build(self) -> reader.TopicSinkReader:
        """Return a TopicSinkReader object."""
        return reader.TopicSinkReader(path=self.path)

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate if the given path is a valid Bagel sink directory."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_dir():
            return False, errors.PathNotDirectoryError(self.path)

        if self._metadata.get("magic") != "BAGEL_SINK":
            return False, errors.InvalidPathError(
                f"{self.path} is not a valid Bagel sink directory."
            )

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
