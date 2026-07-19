import datetime
import pathlib

from bagel.source.ros.parse import LogRecord, looks_like_ros_log, parse_file, parse_line

SAMPLE_DIRECTORY = pathlib.Path("data/sample/ros/log")


def test_parses_ros2_rcutils_line() -> None:
    record = parse_line(
        "[INFO] [1662400000.100000000] [talker]: Publishing: 'Hello World: 1'",
        fallback_node="fallback",
    )

    assert record == LogRecord(
        timestamp_seconds=1662400000.1,
        level="INFO",
        node="talker",
        message="Publishing: 'Hello World: 1'",
        file="",
    )


def test_parses_ros2_launch_line() -> None:
    record = parse_line(
        "1662400025.2000000 [ERROR] [talker-1]: process has died [pid 12345, exit code -6]",
        fallback_node="fallback",
    )

    assert record is not None
    assert record.timestamp_seconds == 1662400025.2
    assert record.level == "ERROR"
    assert record.node == "talker-1"
    assert record.message == "process has died [pid 12345, exit code -6]"


def test_parses_ros1_rosout_line() -> None:
    record = parse_line(
        "1662400015.500000000 WARN /move_base [/tmp/costmap.cpp:88(updateMap)]"
        " [topics: /rosout, /cmd_vel] Costmap update loop missed its desired rate of 5.0 Hz",
        fallback_node="fallback",
    )

    assert record is not None
    assert record.level == "WARN"
    assert record.node == "/move_base"
    assert record.message == "Costmap update loop missed its desired rate of 5.0 Hz"


def test_parses_ros1_roscpp_line_with_fallback_node() -> None:
    record = parse_line(
        "[ INFO] [1662400000.100000000]: waiting for service /spawn",
        fallback_node="turtlesim-1",
    )

    assert record is not None
    assert record.level == "INFO"
    assert record.node == "turtlesim-1"
    assert record.message == "waiting for service /spawn"


def test_parses_rospy_line_with_local_wall_clock() -> None:
    record = parse_line(
        "[rospy.client][WARNING] 2022-09-05 12:00:00,100: shutdown request received",
        fallback_node="fallback",
    )

    assert record is not None
    assert record.level == "WARN"
    assert record.node == "rospy.client"
    assert record.message == "shutdown request received"
    expected = datetime.datetime.strptime(
        "2022-09-05 12:00:00,100", "%Y-%m-%d %H:%M:%S,%f"
    ).timestamp()
    assert record.timestamp_seconds == expected


def test_rejects_non_log_lines() -> None:
    assert parse_line("timestamp,x,y", fallback_node="f") is None
    assert parse_line('{"timestamp": 1.0, "x": 2.0}', fallback_node="f") is None
    assert parse_line("RuntimeError: buffer full", fallback_node="f") is None


def test_parse_file_folds_continuation_lines() -> None:
    records = parse_file(SAMPLE_DIRECTORY / "talker.log")

    assert len(records) == 5
    error = records[3]
    assert error.level == "ERROR"
    assert error.message == (
        "Failed to publish: buffer full"
        "\nTraceback (most recent call last):"
        '\n  File "talker.py", line 42, in publish'
        "\nRuntimeError: buffer full"
    )
    assert all(record.file == "talker.log" for record in records)


def test_looks_like_ros_log_accepts_samples_and_rejects_csv(tmp_path: pathlib.Path) -> None:
    for sample in SAMPLE_DIRECTORY.glob("*.log"):
        assert looks_like_ros_log(sample), sample

    csv_file = tmp_path / "data.log"
    csv_file.write_text("timestamp,x,y\n1.0,2.0,3.0\n")
    assert not looks_like_ros_log(csv_file)
