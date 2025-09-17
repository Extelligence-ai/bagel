"""A topic registry for Betaflight Blackbox logs."""

from src import di
from src.topic.betaflight import bbl


class TopicRegistry(bbl.TopicRegistry):
    """A topic registry for Betaflight Blackbox logs."""


def register() -> None:
    """Register module for dependency injection."""
    di.module_registry[__name__] = TopicRegistry
