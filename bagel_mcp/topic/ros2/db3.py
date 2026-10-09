"""A topic registry for ROS2 sqlite3 bags."""

import pyarrow as pa

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.topic.ros2 import base
from bagel_mcp.topic.ros2.ros2msg import parse, schema

FEATURE = "Describing ROS 2 .db3 bag topics"


class TopicRegistry(base.TopicRegistry):
    """A topic registry for ROS2 sqlite3 bags.

    Message definitions come from the bag itself when it carries them (rosbag2 since
    Jazzy), from the locally installed interfaces inside a ROS 2 image, and from the
    backend's bundled typestore otherwise.
    """

    def struct(self, topic: str, data_source: bags.Reader) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        main, deps = parse.parse(self.describe(topic, data_source))
        return schema.to_pa_struct(main, deps)

    def describe(self, topic: str, data_source: bags.Reader) -> str:
        """Return a human-readable description of the given topic."""
        self._topic_info(topic, data_source)
        return data_source.definition(topic)

    def _metadata(self, data_source: bags.Reader) -> bags.BagInfo:
        return data_source.info


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 2)."""
    bags.require(FEATURE, ros_version=2)
    module.global_registry[__name__] = TopicRegistry
