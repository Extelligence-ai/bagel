"""A logging message dataset for ROS1 bags."""

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.logging import base
from bagel_mcp.message.ros1 import bag


class LoggingDataset(base.TopicBasedLoggingDataset, bag.MessageDataset):
    """A logging message dataset for ROS1 bags."""

    @property
    def type_name(self) -> str:
        """Topic type name that contains logging messages."""
        return "rosgraph_msgs/Log"


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 1)."""
    bags.require("Reading ROS 1 bag logs", ros_version=1)
    module.global_registry[__name__] = LoggingDataset
