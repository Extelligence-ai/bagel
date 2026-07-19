"""A topic registry for PyArrow dataset for JSON files."""

from bagel.di import module
from bagel.topic.pyarrow import base


class TopicRegistry(base.TopicRegistry):
    """A topic registry for PyArrow dataset for JSON files."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
