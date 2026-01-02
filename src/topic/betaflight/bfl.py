"""A topic registry for Betaflight Blackbox logs."""

from src.di import module
from src.topic.betaflight import bbl


class TopicRegistry(bbl.TopicRegistry):
    """A topic registry for Betaflight Blackbox logs."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
