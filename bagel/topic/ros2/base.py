"""A base class for topic registry for ROS2 bags."""

import abc

import pyarrow as pa
import rosbag2_py

from bagel.source.ros2.mcap import McapRos2Bag
from bagel.topic import base

# Shared with the format-agnostic MCAP registry; re-exported here for back-compat.
UnsupportedEncodingError = base.UnsupportedEncodingError
MessageDefinition = base.MessageDefinition


class TopicRegistry(base.TopicRegistry):
    """A base class for topic registry for ROS2 bags."""

    @abc.abstractmethod
    def struct(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""

    @abc.abstractmethod
    def describe(self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader) -> str:
        """Return a human-readable description of the given topic."""

    @abc.abstractmethod
    def _metadata(
        self, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> rosbag2_py.BagMetadata:
        """Return the BagMetadata for the given data source."""

    def available_topics(self, data_source: McapRos2Bag | rosbag2_py.SequentialReader) -> list[str]:
        """Return a list of available topic names."""
        return sorted(
            [
                info.topic_metadata.name
                for info in self._metadata(data_source).topics_with_message_count
            ]
        )

    def native_type_name(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> str:
        """Return the native type name for the given topic."""
        info = self._topic_info(topic, data_source)
        return info.topic_metadata.type

    def message_count(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> int:
        """Return the number of messages for the given topic."""
        info = self._topic_info(topic, data_source)
        return info.message_count

    def _topic_info(
        self, topic: str, data_source: McapRos2Bag | rosbag2_py.SequentialReader
    ) -> rosbag2_py.TopicInformation:
        """Return the TopicInformation for the given topic."""
        for info in self._metadata(data_source).topics_with_message_count:
            if info.topic_metadata.name == topic:
                return info
        raise base.TopicNotFoundError(topic)
