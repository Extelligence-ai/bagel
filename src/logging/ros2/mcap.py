"""A logging message dataset for ROS2 MCAP bags."""

from src.di import module
from src.logging import base
from src.message.ros2 import mcap


class LoggingDataset(base.TopicBasedLoggingDataset, mcap.MessageDataset):
    """A logging message dataset for ROS2 MCAP bags."""

    @property
    def type_name(self) -> str:
        """Topic type name that contains logging messages."""
        return "rcl_interfaces/msg/Log"


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
