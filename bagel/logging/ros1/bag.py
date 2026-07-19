"""A logging message dataset for ROS1 bags."""

from bagel.di import module
from bagel.logging import base
from bagel.message.ros1 import bag


class LoggingDataset(base.TopicBasedLoggingDataset, bag.MessageDataset):
    """A logging message dataset for ROS1 bags."""

    @property
    def type_name(self) -> str:
        """Topic type name that contains logging messages."""
        return "rosgraph_msgs/Log"


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
