"""A topic registry for PyArrow dataset for JSON files."""

from bagel_mcp.di import module
from bagel_mcp.topic.pyarrow import base


class TopicRegistry(base.TopicRegistry):
    """A topic registry for PyArrow dataset for JSON files."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
