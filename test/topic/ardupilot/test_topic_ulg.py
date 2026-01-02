from src.source.ardupilot.bin import SourceFactory
from src.topic.ardupilot.bin import TopicRegistry


def test_topic_registry() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    data_source = factory.build()

    # WHEN
    registry = TopicRegistry()

    # THEN
    assert registry.available_topics(data_source) == [
        "AETR",
        "AHR2",
        "AOA",
        "ARM",
        "ATT",
        "AUXF",
        "BARO",
        "BAT",
        "CAND",
        "CANS",
        "CMD",
        "CTUN",
        "DCM",
        "DSF",
        "EAHR",
        "EV",
        "FILE",
        "FMT",
        "FMTU",
        "GPA",
        "GPS",
        "HEAT",
        "IMU",
        "IOMC",
        "LAND",
        "MAG",
        "MAV",
        "MCU",
        "MODE",
        "MSG",
        "MULT",
        "NTUN",
        "ORGN",
        "PARM",
        "PIDP",
        "PIDR",
        "PM",
        "POS",
        "POWR",
        "RCI2",
        "RCIN",
        "RCO2",
        "RCOU",
        "STAK",
        "STAT",
        "TEC2",
        "TECS",
        "TERR",
        "TSYN",
        "UART",
        "UNIT",
        "VER",
        "VIBE",
        "XKF1",
        "XKF2",
        "XKF3",
        "XKF4",
        "XKF5",
        "XKFS",
        "XKQ",
        "XKT",
        "XKV1",
        "XKV2",
        "XKY0",
        "XKY1",
    ]
    assert registry.native_type_name("AHR2", data_source) == "AHR2"
    assert registry.message_count("AHR2", data_source) == 1028
    # `registry.struct()` and `registry.describe()` are NOT tested here
    # because they require network access to download .xml files.
    # This will make them very flaky in CI/CD environment.
