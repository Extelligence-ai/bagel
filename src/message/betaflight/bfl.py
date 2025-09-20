"""A message dataset for Betaflight Blackbox logs."""

from src.di import module
from src.message.betaflight import bbl


class MessageDataset(bbl.MessageDataset):
    """A message dataset for Betaflight Blackbox logs."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
