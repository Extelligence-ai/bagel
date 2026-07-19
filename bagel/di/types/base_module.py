"""Base module keys for dependency injection."""

from enum import Enum


class BaseModule(Enum):
    """Base module keys for dependency injection."""

    SOURCE_FACTORY = "bagel.source"
    TOPIC_REGISTRY = "bagel.topic"
    MESSAGE_DATASET = "bagel.message"
    IMAGE_DATASET = "bagel.image"
    LOGGING_DATASET = "bagel.logging"
    TOPIC_SINK = "bagel.sink"
