"""Base module keys for dependency injection."""

from enum import Enum


class BaseModule(Enum):
    """Base module keys for dependency injection."""

    SOURCE_FACTORY = "src.source"
    TOPIC_REGISTRY = "src.topic"
    MESSAGE_DATASET = "src.message"
    LOGGING_DATASET = "src.logging"
