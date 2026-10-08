"""A logging message dataset for ROS2 sqlite3 bags."""

from bagel_mcp.di import module
from bagel_mcp.logging import base
from bagel_mcp.message.ros2 import db3


class LoggingDataset(base.TopicBasedLoggingDataset, db3.MessageDataset):
    """A logging message dataset for ROS2 sqlite3 bags."""

    @property
    def type_name(self) -> str:
        """Topic type name that contains logging messages."""
        return "rcl_interfaces/msg/Log"


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
