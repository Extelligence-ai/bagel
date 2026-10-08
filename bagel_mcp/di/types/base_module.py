"""Base module keys for dependency injection."""

from enum import Enum


class BaseModule(Enum):
    """Base module keys for dependency injection."""

    SOURCE_FACTORY = "bagel_mcp.source"
    TOPIC_REGISTRY = "bagel_mcp.topic"
    MESSAGE_DATASET = "bagel_mcp.message"
    IMAGE_DATASET = "bagel_mcp.image"
    LOGGING_DATASET = "bagel_mcp.logging"
    TOPIC_SINK = "bagel_mcp.sink"
