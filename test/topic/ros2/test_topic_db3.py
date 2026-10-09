import pyarrow as pa

from bagel_mcp import ros_native
from bagel_mcp.source.ros2.db3 import SourceFactory
from bagel_mcp.topic.ros2.db3 import TopicRegistry


def test_topic_registry() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3")
    data_source = factory.build()

    # WHEN
    registry = TopicRegistry()

    # THEN
    assert registry.available_topics(data_source) == [
        "AAA",
        "BBB",
        "CCC",
        "DDD",
        "EEE",
        "FFF",
        "GGG",
        "HHH",
    ]
    assert registry.native_type_name("AAA", data_source) == "std_msgs/msg/String"
    assert registry.message_count("AAA", data_source) == 804
    assert registry.struct("AAA", data_source) == pa.struct([pa.field("data", pa.string(), False)])
    described = registry.describe("AAA", data_source)
    assert "string data" in described
    if ros_native.available("rosidl_runtime_py"):
        # Inside a ROS 2 image the locally installed interface, comments included,
        # is preferred over the backend's bundled (comment-free) definition.
        assert described.startswith("# This was originally provided as an example message.\n")
