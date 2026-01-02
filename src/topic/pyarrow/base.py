"""A topic registry for PyArrow dataset."""

import pyarrow as pa

from src.source.pyarrow.base import PyArrowDataset
from src.topic import base

TOPIC_NAME = "message"
TOPIC_TYPE_NAME = "message"


class TopicRegistry(base.TopicRegistry):
    """A topic registry for PyArrow dataset for files.

    Notes:
        There is no "topic" concept in PyArrow datasets. This registry treats the entire
        dataset as a single topic named "message".

    """

    def available_topics(self, data_source: PyArrowDataset) -> list[str]:
        """Return a list of available topic names."""
        return [TOPIC_NAME]

    def native_type_name(self, topic: str, data_source: PyArrowDataset) -> str:
        """Return the native type name for the given topic."""
        return TOPIC_TYPE_NAME

    def message_count(self, topic: str, data_source: PyArrowDataset) -> None:
        """Return the number of messages for the given topic."""
        return None  # unbounded

    def struct(self, topic: str, data_source: PyArrowDataset) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        return pa.struct(data_source.dataset.schema)

    def describe(self, topic: str, data_source: PyArrowDataset) -> None:
        """Return a human-readable description of the given topic."""
        return None
