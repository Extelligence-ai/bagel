"""A topic registry for ROS1 bags."""

from typing import Any

import pyarrow as pa
import rosbag
import yaml

from src import di
from src.topic import base
from src.topic.ros1 import parse, schema


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ROS1 bags."""

    def available_topics(self, data_source: rosbag.Bag) -> list[str]:
        """Return a list of available topic names."""
        return sorted(
            [info["topic"] for info in yaml.safe_load(data_source._get_yaml_info())["topics"]]
        )

    def native_type_name(self, topic: str, data_source: rosbag.Bag) -> str:
        """Return the native type name for the given topic."""
        return str(self._topic_info(topic, data_source)["type"])

    def message_count(self, topic: str, data_source: rosbag.Bag) -> int:
        """Return the number of messages for the given topic."""
        return int(self._topic_info(topic, data_source)["messages"])

    def struct(self, topic: str, data_source: rosbag.Bag) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        full_text = self.describe(topic, data_source)
        main, deps = parse.parse(full_text)
        return schema.to_pa_struct(main, deps)

    def describe(self, topic: str, data_source: rosbag.Bag) -> str:
        """Return a human-readable description of the given topic."""
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)

        _, msg, _ = next(data_source.read_messages([topic]))
        return msg._full_text

    def _topic_info(self, topic: str, data_source: rosbag.Bag) -> dict[str, Any]:
        """Return the topic info dictionary for the given topic."""
        metadata = yaml.safe_load(data_source._get_yaml_info())
        for info in metadata["topics"]:
            if info["topic"] == topic:
                return info
        raise base.TopicNotFoundError(topic)


def register() -> None:
    """Register module for dependency injection."""
    di.module_registry[__name__] = TopicRegistry
