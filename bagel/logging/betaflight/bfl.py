"""A logging message dataset for Betaflight Blackbox logs."""

from bagel.di import module
from bagel.logging.betaflight import bbl


class LoggingDataset(bbl.LoggingDataset):
    """A logging message dataset for Betaflight Blackbox logs."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = LoggingDataset
