"""A base class for topic registry for ROS2 bags."""

import abc

import pyarrow as pa

from bagel_mcp import bags
from bagel_mcp.source.ros2.mcap import McapRos2Bag
from bagel_mcp.topic import base

# Shared with the format-agnostic MCAP registry; re-exported here for back-compat.
UnsupportedEncodingError = base.UnsupportedEncodingError
MessageDefinition = base.MessageDefinition


class TopicRegistry(base.TopicRegistry):
    """A base class for topic registry for ROS2 bags."""

    @abc.abstractmethod
    def struct(self, topic: str, data_source: McapRos2Bag | bags.Reader) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""

    @abc.abstractmethod
    def describe(self, topic: str, data_source: McapRos2Bag | bags.Reader) -> str:
        """Return a human-readable description of the given topic."""

    @abc.abstractmethod
    def _metadata(self, data_source: McapRos2Bag | bags.Reader) -> bags.BagInfo:
        """Return the bag metadata for the given data source."""

    def available_topics(self, data_source: McapRos2Bag | bags.Reader) -> list[str]:
        """Return a list of available topic names."""
        return self._metadata(data_source).topic_names

    def native_type_name(self, topic: str, data_source: McapRos2Bag | bags.Reader) -> str:
        """Return the native type name for the given topic."""
        return self._topic_info(topic, data_source).type_name

    def message_count(self, topic: str, data_source: McapRos2Bag | bags.Reader) -> int:
        """Return the number of messages for the given topic."""
        return self._topic_info(topic, data_source).message_count

    def _topic_info(self, topic: str, data_source: McapRos2Bag | bags.Reader) -> bags.TopicInfo:
        """Return the topic's recorded metadata."""
        try:
            return self._metadata(data_source).topic(topic)
        except bags.UnknownTopicError as error:
            raise base.TopicNotFoundError(topic) from error
