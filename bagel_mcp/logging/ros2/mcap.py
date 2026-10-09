"""A logging message dataset for ROS2 MCAP bags."""

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.logging import base
from bagel_mcp.message.ros2 import mcap


class LoggingDataset(base.TopicBasedLoggingDataset, mcap.MessageDataset):
    """A logging message dataset for ROS2 MCAP bags."""

    @property
    def type_name(self) -> str:
        """Topic type name that contains logging messages."""
        return "rcl_interfaces/msg/Log"


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 2)."""
    bags.require("Reading ROS 2 MCAP bag logs through rosbag2 metadata", ros_version=2)
    module.global_registry[__name__] = LoggingDataset
