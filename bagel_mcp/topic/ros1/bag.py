"""A topic registry for ROS1 bags."""

import pyarrow as pa

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.topic import base
from bagel_mcp.topic.ros1 import parse, schema

FEATURE = "Describing ROS 1 .bag topics"


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ROS1 bags."""

    def available_topics(self, data_source: bags.Reader) -> list[str]:
        """Return a list of available topic names."""
        return data_source.info.topic_names

    def native_type_name(self, topic: str, data_source: bags.Reader) -> str:
        """Return the native type name for the given topic."""
        return self._topic_info(topic, data_source).type_name

    def message_count(self, topic: str, data_source: bags.Reader) -> int:
        """Return the number of messages for the given topic."""
        return self._topic_info(topic, data_source).message_count

    def struct(self, topic: str, data_source: bags.Reader) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        full_text = self.describe(topic, data_source)
        main, deps = parse.parse(full_text)
        return schema.to_pa_struct(main, deps)

    def describe(self, topic: str, data_source: bags.Reader) -> str:
        """Return a human-readable description of the given topic."""
        self._topic_info(topic, data_source)
        return data_source.definition(topic)

    def _topic_info(self, topic: str, data_source: bags.Reader) -> bags.TopicInfo:
        """Return the topic's recorded metadata."""
        try:
            return data_source.info.topic(topic)
        except bags.UnknownTopicError as error:
            raise base.TopicNotFoundError(topic) from error


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 1)."""
    bags.require(FEATURE, ros_version=1)
    module.global_registry[__name__] = TopicRegistry
