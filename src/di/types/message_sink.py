"""A list of supported message sink types and utilities to identify them."""

from enum import Enum

from settings import settings


class TopicSink(Enum):
    """Supported message sink types."""

    ROS2_BRIDGE = "ros2.bridge"


def guess_host(type_: TopicSink) -> str:
    """Guess the default host for the given TopicSink type."""
    match type_:
        case TopicSink.ROS2_BRIDGE if settings.CONTAINER_MODE:
            return "host.docker.internal"
        case TopicSink.ROS2_BRIDGE:
            return "localhost"
        case _:
            raise ValueError(
                f"Cannot guess host for TopicSink type: {type_}. "
                f"Available options are: {', '.join([t.value for t in TopicSink])}"
            )


def guess_port(type_: TopicSink) -> int:
    """Guess the default port for the given TopicSink type."""
    match type_:
        case TopicSink.ROS2_BRIDGE:
            return 9090
        case _:
            raise ValueError(
                f"Cannot guess port for TopicSink type: {type_}. "
                f"Available options are: {', '.join([t.value for t in TopicSink])}"
            )
