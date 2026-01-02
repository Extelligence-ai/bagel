import pyarrow as pa

from src.source.ros2.mcap import SourceFactory
from src.topic.ros2.mcap import TopicRegistry


def test_topic_registry() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap")
    data_source = factory.build()

    # WHEN
    registry = TopicRegistry()

    # THEN
    assert registry.available_topics(data_source) == [
        "/events/write_split",
        "/fluid_pressure",
        "/parameter_events",
        "/rosout",
    ]
    assert (
        registry.native_type_name("/fluid_pressure", data_source) == "sensor_msgs/msg/FluidPressure"
    )
    assert registry.message_count("/fluid_pressure", data_source) == 7
    assert registry.struct("/fluid_pressure", data_source) == pa.struct(
        [
            pa.field(
                "header",
                pa.struct(
                    [
                        pa.field(
                            "stamp",
                            pa.struct(
                                [
                                    pa.field("sec", pa.int32(), False),
                                    pa.field("nanosec", pa.uint32(), False),
                                ]
                            ),
                            False,
                        ),
                        pa.field("frame_id", pa.string(), False),
                    ]
                ),
                False,
            ),
            pa.field("fluid_pressure", pa.float64(), False),
            pa.field("variance", pa.float64(), False),
        ]
    )
    assert "MSG:" in registry.describe("/fluid_pressure", data_source)
