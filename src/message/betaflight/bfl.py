"""A message dataset for Betaflight Blackbox logs."""

from src import di
from src.message.betaflight import bbl


class MessageDataset(bbl.MessageDataset):
    """A message dataset for Betaflight Blackbox logs."""


def register() -> None:
    """Register module for dependency injection."""
    di.module_registry[__name__] = MessageDataset
