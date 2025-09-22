"""A logging message dataset for ArduPilot Dataflash logs."""

from src.di import module
from src.logging import base
from src.message.ardupilot.bin import MessageDataset


class LoggingDataset(base.TopicBasedLoggingDataset, MessageDataset):
    """A logging message dataset for ArduPilot Dataflash logs."""

    @property
    def type_name(self) -> str:
        """Topic type name that contains logging messages."""
        return "MSG"


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
