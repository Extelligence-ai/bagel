"""A logging message dataset for MCAP files.

For ROS2-profile MCAP files the log topic type is `rcl_interfaces/msg/Log`; other
profiles that record logs under the same schema name work identically.
"""

from src.di import module
from src.logging import base
from src.message import mcap


class LoggingDataset(base.TopicBasedLoggingDataset, mcap.MessageDataset):
    """A logging message dataset for MCAP files."""

    @property
    def type_name(self) -> str:
        """Topic type name that contains logging messages."""
        return "rcl_interfaces/msg/Log"


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
