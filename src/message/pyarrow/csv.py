"""A message dataset for PyArrow dataset for CSV files."""

from src.di import module
from src.message.pyarrow import base


class MessageDataset(base.MessageDataset):
    """A message dataset for PyArrow dataset for CSV files."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
