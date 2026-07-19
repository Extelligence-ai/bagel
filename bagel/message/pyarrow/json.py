"""A message dataset for PyArrow dataset for JSON files."""

from bagel.di import module
from bagel.message.pyarrow import base


class MessageDataset(base.MessageDataset):
    """A message dataset for PyArrow dataset for JSON files."""


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = MessageDataset
