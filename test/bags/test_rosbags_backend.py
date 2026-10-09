"""The pure-Python bag backend reads the bundled bags and round-trips what it writes."""

import dataclasses
import pathlib

import pytest

from bagel_mcp import bags
from bagel_mcp.bags import base
from bagel_mcp.settings import settings
from test._fixtures import ros_bags

pytest.importorskip("rosbags")


@pytest.fixture(autouse=True)
def _rosbags_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "BAG_BACKEND", "rosbags")


# ---------------------------------------------------------------------------- reading


def test_ros1_sample_bag_info_matches_rosbag_info() -> None:
    reader = bags.open_reader("data/sample/ros1/sample.bag", ros_version=1)
    info = reader.info
    assert info.ros_version == 1
    assert info.version == "2.0"
    assert info.compression_format == "none"
    assert info.message_count == 34
    assert info.start_ns == 1660676075822089758
    assert info.topic_names == ["/rosout", "/turtle1/cmd_vel"]
    twist = info.topic("/turtle1/cmd_vel")
    assert twist.type_name == "geometry_msgs/Twist"  # ROS 1 spelling, not rosbags' pkg/msg/Name
    assert twist.message_count == 33
    assert twist.digest == "9f195f881246fdfa2798d1d3eebca84a"
    # The recorded definition, comments included, is what `rosbag` showed too.
    assert reader.definition("/turtle1/cmd_vel").startswith("# This expresses velocity")


@pytest.mark.parametrize(
    ("path", "count", "storage", "compression"),
    [
        ("data/sample/ros2/db3", 6074, "sqlite3", ("", "")),
        ("data/sample/ros2/db3/part_0.db3", 1246, "sqlite3", ("", "")),
        ("data/sample/ros2/db3_zstd", 6, "sqlite3", ("zstd", "file")),
        ("data/sample/ros2/db3_zstd/part_0.db3.zstd", 6, "sqlite3", ("", "")),
        ("data/sample/ros2/mcap", 15, "mcap", ("", "")),
        ("data/sample/ros2/mcap_zstd", 6, "mcap", ("zstd", "file")),
    ],
)
def test_ros2_sample_bags_open_in_every_layout(
    path: str, count: int, storage: str, compression: tuple[str, str]
) -> None:
    reader = bags.open_reader(path, ros_version=2)
    info = reader.info
    assert info.ros_version == 2
    assert info.storage_identifier == storage
    assert (info.compression_format, info.compression_mode) == compression
    assert info.message_count == count
    assert sum(1 for _ in reader.raw_messages(info.topic_names, None, None)) == count
    assert sum(topic.message_count for topic in info.topics) == count


def test_ros2_definitions_fall_back_to_the_bundled_typestore() -> None:
    # The version-4 sample bag carries no message definitions.
    reader = bags.open_reader("data/sample/ros2/db3", ros_version=2)
    assert reader.definition("AAA").strip() == "string data"
    # A bag that carries them returns them verbatim (comments included).
    reader = bags.open_reader("data/sample/ros2/db3_zstd", ros_version=2)
    assert reader.definition("/rosout").startswith("##")
    with pytest.raises(bags.UnknownTopicError):
        reader.definition("/nope")


def test_ros2_messages_deserialize_with_time_bounds() -> None:
    reader = bags.open_reader("data/sample/ros2/db3", ros_version=2)
    first = next(iter(reader.messages(["AAA"], None, None)))
    assert first[0] == "AAA"
    assert first[2].data.startswith("Hello, world!")
    # Bounds are inclusive seconds; the sample's stamps are 1000..3035 ns.
    bounded = list(reader.raw_messages(reader.info.topic_names, 1e-06, 2e-06))
    assert bounded
    assert all(1000 <= timestamp_ns <= 2000 for _, timestamp_ns, _ in bounded)


def test_ros1_time_bounds_follow_genpy_rounding() -> None:
    """Bounds in seconds become `genpy.Time.from_sec` nanoseconds, as `rosbag` applied them."""
    reader = bags.open_reader("data/sample/ros1/sample.bag", ros_version=1)
    stamps = [ns for _, ns, _ in reader.raw_messages(["/turtle1/cmd_vel"], None, None)]
    boundary = base.ros1_seconds(stamps[4])
    boundary_ns = base.ros1_nanoseconds(boundary)
    kept = [ns for _, ns, _ in reader.raw_messages(["/turtle1/cmd_vel"], None, boundary)]
    assert kept == [ns for ns in stamps if ns <= boundary_ns]
    kept = [ns for _, ns, _ in reader.raw_messages(["/turtle1/cmd_vel"], boundary, None)]
    assert kept == [ns for ns in stamps if ns >= boundary_ns]
    assert base.ros1_seconds(base.ros1_nanoseconds(1660676079.0116584) or 0) == 1660676079.0116584


def test_missing_storage_files_are_a_storage_error(tmp_path: pathlib.Path) -> None:
    (tmp_path / "metadata.yaml").write_text(
        pathlib.Path("data/sample/ros2/mcap/metadata.yaml").read_text(encoding="utf-8")
    )
    with pytest.raises(bags.BagStorageError):
        bags.open_reader(tmp_path, ros_version=2)


# ---------------------------------------------------------------------------- writing


def _copy(source: bags.Reader, writer: bags.Writer) -> None:
    for name in source.info.topic_names:
        writer.add_topic(
            dataclasses.replace(source.info.topic(name), definition=source.definition(name))
        )
    for topic, timestamp_ns, data in source.raw_messages(source.info.topic_names, None, None):
        writer.write(topic, timestamp_ns, data)


@pytest.mark.parametrize(
    ("storage", "compression"),
    [
        ("sqlite3", None),
        ("sqlite3", ("file", "zstd")),
        ("sqlite3", ("message", "zstd")),
        ("mcap", None),
    ],
)
def test_ros2_copies_round_trip_byte_for_byte(
    tmp_path: pathlib.Path, storage: str, compression: tuple[str, str] | None
) -> None:
    source = bags.open_reader("data/sample/ros2/db3_zstd", ros_version=2)
    out = tmp_path / "copy"
    with bags.open_writer(out, ros_version=2, storage=storage, compression=compression) as writer:
        _copy(source, writer)
    copy = bags.open_reader(out, ros_version=2)
    assert copy.info.storage_identifier == storage
    assert (copy.info.compression_format, copy.info.compression_mode) == (
        compression[1] if compression else "",
        compression[0] if compression else "",
    )
    assert copy.info.version == "8"  # readable by every supported distro's `ros2 bag info`
    expected = list(source.raw_messages(source.info.topic_names, None, None))
    assert list(copy.raw_messages(copy.info.topic_names, None, None)) == expected
    # The type digest and definition travel with the copy.
    assert copy.info.topic("/rosout").digest == source.info.topic("/rosout").digest
    assert copy.definition("/rosout") == source.definition("/rosout")


@pytest.mark.parametrize("compression", [None, "bz2", "lz4"])
def test_ros1_copies_round_trip_byte_for_byte(
    tmp_path: pathlib.Path, compression: str | None
) -> None:
    source = bags.open_reader("data/sample/ros1/sample.bag", ros_version=1)
    out = tmp_path / "copy.bag"
    with bags.open_writer(out, ros_version=1, compression=compression) as writer:
        _copy(source, writer)
    copy = bags.open_reader(out, ros_version=1)
    assert copy.info.compression_format == (compression or "none")
    assert [(t.name, t.type_name, t.message_count, t.digest) for t in copy.info.topics] == [
        (t.name, t.type_name, t.message_count, t.digest) for t in source.info.topics
    ]
    expected = list(source.raw_messages(source.info.topic_names, None, None))
    assert list(copy.raw_messages(copy.info.topic_names, None, None)) == expected
    assert copy.definition("/turtle1/cmd_vel") == source.definition("/turtle1/cmd_vel")


def test_synthetic_bags_decode_the_planted_events(tmp_path: pathlib.Path) -> None:
    ros2 = bags.open_reader(ros_bags.write_ros2_imu_bag(tmp_path / "ros2"), ros_version=2)
    ros1 = bags.open_reader(ros_bags.write_ros1_imu_bag(tmp_path / "ros1.bag"), ros_version=1)
    for reader in (ros2, ros1):
        assert reader.info.topic_names == ["/imu", "/status"]
        dips = [
            base.ros2_seconds(timestamp_ns) - ros_bags.EPOCH
            for _, timestamp_ns, message in reader.messages(["/imu"], None, None)
            if message.linear_acceleration.x < -10
        ]
        assert min(dips) == pytest.approx(ros_bags.EVENTS[0][0])
        assert max(dips) == pytest.approx(ros_bags.EVENTS[1][1])
