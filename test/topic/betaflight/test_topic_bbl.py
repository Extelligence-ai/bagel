import pyarrow as pa

from src.source.betaflight.bbl import SourceFactory
from src.topic.betaflight.bbl import TopicRegistry


def test_topic_registry() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/betaflight/sample.bbl", log_index=1)
    data_source = factory.build()

    # WHEN
    registry = TopicRegistry()

    # THEN
    assert registry.available_topics(data_source) == ["I", "P"]
    assert registry.native_type_name("I", data_source) == "INTRA"
    assert registry.message_count("I", data_source) == 3246
    assert registry.struct("I", data_source) == pa.struct(
        [
            pa.field("loopIteration", pa.int64(), False),
            pa.field("time", pa.int64(), False),
            pa.field("axisP[0]", pa.int64(), False),
            pa.field("axisP[1]", pa.int64(), False),
            pa.field("axisP[2]", pa.int64(), False),
            pa.field("axisI[0]", pa.int64(), False),
            pa.field("axisI[1]", pa.int64(), False),
            pa.field("axisI[2]", pa.int64(), False),
            pa.field("axisD[0]", pa.int64(), False),
            pa.field("axisD[1]", pa.int64(), False),
            pa.field("axisF[0]", pa.int64(), False),
            pa.field("axisF[1]", pa.int64(), False),
            pa.field("axisF[2]", pa.int64(), False),
            pa.field("rcCommand[0]", pa.int64(), False),
            pa.field("rcCommand[1]", pa.int64(), False),
            pa.field("rcCommand[2]", pa.int64(), False),
            pa.field("rcCommand[3]", pa.int64(), False),
            pa.field("setpoint[0]", pa.int64(), False),
            pa.field("setpoint[1]", pa.int64(), False),
            pa.field("setpoint[2]", pa.int64(), False),
            pa.field("setpoint[3]", pa.int64(), False),
            pa.field("vbatLatest", pa.int64(), False),
            pa.field("amperageLatest", pa.int64(), False),
            pa.field("baroAlt", pa.int64(), False),
            pa.field("rssi", pa.int64(), False),
            pa.field("gyroADC[0]", pa.int64(), False),
            pa.field("gyroADC[1]", pa.int64(), False),
            pa.field("gyroADC[2]", pa.int64(), False),
            pa.field("gyroUnfilt[0]", pa.int64(), False),
            pa.field("gyroUnfilt[1]", pa.int64(), False),
            pa.field("gyroUnfilt[2]", pa.int64(), False),
            pa.field("accSmooth[0]", pa.int64(), False),
            pa.field("accSmooth[1]", pa.int64(), False),
            pa.field("accSmooth[2]", pa.int64(), False),
            pa.field("debug[0]", pa.int64(), False),
            pa.field("debug[1]", pa.int64(), False),
            pa.field("debug[2]", pa.int64(), False),
            pa.field("debug[3]", pa.int64(), False),
            pa.field("debug[4]", pa.int64(), False),
            pa.field("debug[5]", pa.int64(), False),
            pa.field("debug[6]", pa.int64(), False),
            pa.field("debug[7]", pa.int64(), False),
            pa.field("motor[0]", pa.int64(), False),
            pa.field("motor[1]", pa.int64(), False),
            pa.field("motor[2]", pa.int64(), False),
            pa.field("motor[3]", pa.int64(), False),
            pa.field("eRPM[0]", pa.int64(), False),
            pa.field("eRPM[1]", pa.int64(), False),
            pa.field("eRPM[2]", pa.int64(), False),
            pa.field("eRPM[3]", pa.int64(), False),
        ]
    )
    assert registry.describe("I", data_source).startswith("# Intraframe I\n")
