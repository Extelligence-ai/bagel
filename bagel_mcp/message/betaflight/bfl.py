"""A message dataset for Betaflight Blackbox logs."""

from bagel_mcp.di import module
from bagel_mcp.message.betaflight import bbl


class MessageDataset(bbl.MessageDataset):
    """A message dataset for Betaflight Blackbox logs."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
