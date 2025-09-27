import pyarrow as pa

from src.source.ros1.bag import SourceFactory
from src.topic.ros1.bag import TopicRegistry


def test_topic_registry() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros1/sample.bag")
    data_source = factory.build()

    # WHEN
    registry = TopicRegistry()

    # THEN
    assert registry.available_topics(data_source) == ["/rosout", "/turtle1/cmd_vel"]
    assert registry.native_type_name("/turtle1/cmd_vel", data_source) == "geometry_msgs/Twist"
    assert registry.message_count("/turtle1/cmd_vel", data_source) == 33
    assert registry.struct("/turtle1/cmd_vel", data_source) == pa.struct(
        [
            pa.field(
                "linear",
                pa.struct(
                    [
                        pa.field("x", pa.float64(), False),
                        pa.field("y", pa.float64(), False),
                        pa.field("z", pa.float64(), False),
                    ]
                ),
                False,
            ),
            pa.field(
                "angular",
                pa.struct(
                    [
                        pa.field("x", pa.float64(), False),
                        pa.field("y", pa.float64(), False),
                        pa.field("z", pa.float64(), False),
                    ]
                ),
                False,
            ),
        ]
    )
    assert registry.describe("/turtle1/cmd_vel", data_source).startswith(
        "# This expresses velocity in free space broken into its linear and angular parts.\n"
    )
