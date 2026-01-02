"""A topic registry for PyArrow dataset for JSON files."""

from src.di import module
from src.topic.pyarrow import base


class TopicRegistry(base.TopicRegistry):
    """A topic registry for PyArrow dataset for JSON files."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
