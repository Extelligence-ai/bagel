"""Native ROS vs the pure-Python `rosbags` backend on the same bags, inside the ROS images.

Each fixture is opened through both backends (``BAG_BACKEND=native`` / ``rosbags``) and
must agree on topics, message counts, every timestamp, the serialized bytes and the
decoded values. Bags the `rosbags` writer produces (the reduce / snippet outputs) must
open with the native reader and with the ``ros2 bag info`` / ``rosbag info`` CLIs.
Compression is covered on both sides: file- and message-level zstd for rosbag2,
bz2 and lz4 for ROS 1.

Runs where the native stack is: the ROS 2 cases skip outside a ROS 2 image and the
ROS 1 cases outside the ROS 1 image.
"""

from __future__ import annotations

import dataclasses
import os
import pathlib
import shutil
import subprocess

import pytest

from bagel_mcp import bags, ros_native
from bagel_mcp.message.ros1 import convert as ros1_convert
from bagel_mcp.message.ros2 import convert as ros2_convert
from bagel_mcp.pipeline import base as pipeline_base
from bagel_mcp.settings import settings
from bagel_mcp.topic.ros1 import parse as ros1_parse
from bagel_mcp.topic.ros1 import schema as ros1_schema
from bagel_mcp.topic.ros2.ros2msg import parse as ros2_parse
from bagel_mcp.topic.ros2.ros2msg import schema as ros2_schema
from test._fixtures import ros_bags

pytest.importorskip("rosbags")
if not (ros_native.available("rosbag2_py") or ros_native.available("rosbag")):
    # One module-level skip on the host (conftest allows it under
    # BAGEL_REQUIRE_OPTIONAL_TESTS); inside an image the per-test markers below
    # pick the ROS 1 or ROS 2 cases.
    pytest.skip("native ROS stack: the ROS service images only", allow_module_level=True)
pytestmark = pytest.mark.integration

ROS_DISTRO = os.getenv("ROS_DISTRO", "")
NATIVE_ZSTD = ROS_DISTRO not in ("humble", "iron")  # rosbag2 zstd arrived after Iron

needs_ros2 = pytest.mark.skipif(
    not ros_native.available("rosbag2_py"), reason="native rosbag2_py: ROS 2 images only"
)
needs_ros1 = pytest.mark.skipif(
    not ros_native.available("rosbag"), reason="native rosbag: the ROS 1 image only"
)


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def _open(path: pathlib.Path | str, ros_version: int, backend: str) -> bags.Reader:
    previous = settings.BAG_BACKEND
    settings.BAG_BACKEND = backend
    try:
        return bags.open_reader(path, ros_version)
    finally:
        settings.BAG_BACKEND = previous


def _struct(reader: bags.Reader, topic: str) -> object:
    definition = reader.definition(topic)
    if reader.info.ros_version == 1:
        main, deps = ros1_parse.parse(definition)
        return ros1_schema.to_pa_struct(main, deps)
    main, deps = ros2_parse.parse(definition)
    return ros2_schema.to_pa_struct(main, deps)


def _decoded(reader: bags.Reader, topic: str) -> list[dict]:
    struct = _struct(reader, topic)
    convert = ros1_convert if reader.info.ros_version == 1 else ros2_convert
    return [
        convert.to_json(message, struct) for _, _, message in reader.messages([topic], None, None)
    ]


def assert_parity(path: pathlib.Path | str, ros_version: int) -> None:
    """The two backends must agree on everything the datasets consume."""
    native = _open(path, ros_version, "native")
    pure = _open(path, ros_version, "rosbags")

    # Topics: names, types, counts; the bag-level counts and bounds.
    assert pure.info.topic_names == native.info.topic_names
    for name in native.info.topic_names:
        assert (pure.info.topic(name).type_name, pure.info.topic(name).message_count) == (
            native.info.topic(name).type_name,
            native.info.topic(name).message_count,
        ), name
    assert pure.info.message_count == native.info.message_count
    assert pure.info.start_ns == native.info.start_ns
    assert pure.info.end_ns == native.info.end_ns

    # Every message: topic order, timestamp, and the recorded bytes.
    topics = native.info.topic_names
    assert list(pure.raw_messages(topics, None, None)) == list(
        native.raw_messages(topics, None, None)
    )

    # Decoded values, field by field, through the same schema.
    for name in topics:
        assert _decoded(pure, name) == _decoded(native, name), name

    # Time bounds: a window from the third to the sixth message of the first topic.
    stamps = [ns for _, ns, _ in native.raw_messages(topics[:1], None, None)]
    if len(stamps) >= 6:
        seconds = bags.base.ros1_seconds if ros_version == 1 else bags.base.ros2_seconds
        start, end = seconds(stamps[2]), seconds(stamps[5])
        assert list(pure.raw_messages(topics, start, end)) == list(
            native.raw_messages(topics, start, end)
        )


# ---------------------------------------------------------------------------- ROS 2


@needs_ros2
@pytest.mark.parametrize(
    "path",
    ["data/sample/ros2/db3", "data/sample/ros2/db3/part_0.db3", "data/sample/ros2/mcap"],
)
def test_ros2_sample_bags_agree(path: str) -> None:
    assert_parity(path, ros_version=2)


@needs_ros2
@pytest.mark.skipif(not NATIVE_ZSTD, reason="native rosbag2 has no zstd on this distro")
@pytest.mark.parametrize("path", ["data/sample/ros2/db3_zstd", "data/sample/ros2/mcap_zstd"])
def test_ros2_compressed_sample_bags_agree(path: str) -> None:
    assert_parity(path, ros_version=2)


@needs_ros2
@pytest.mark.parametrize(
    ("storage", "compression"),
    [
        ("sqlite3", None),
        ("mcap", None),
        pytest.param(
            "sqlite3",
            ("file", "zstd"),
            marks=pytest.mark.skipif(not NATIVE_ZSTD, reason="no native zstd"),
        ),
        pytest.param(
            "sqlite3",
            ("message", "zstd"),
            marks=pytest.mark.skipif(not NATIVE_ZSTD, reason="no native zstd"),
        ),
    ],
)
def test_ros2_bags_written_by_rosbags_read_natively(
    tmp_path: pathlib.Path, storage: str, compression: tuple[str, str] | None
) -> None:
    bag = ros_bags.write_ros2_imu_bag(tmp_path / "synth", storage, compression, ROS_DISTRO)
    assert_parity(bag, ros_version=2)
    info = subprocess.run(  # noqa: S603 -- the ROS CLI on a path this test wrote
        ["ros2", "bag", "info", str(bag)],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    assert info.returncode == 0, info.stderr
    assert "/imu" in info.stdout


@needs_ros2
def test_reduce_and_snippet_outputs_open_natively_and_with_the_cli(
    tmp_path: pathlib.Path,
) -> None:
    source = ros_bags.write_ros2_imu_bag(tmp_path / "synth", "sqlite3")
    reduce_config = {
        "name": "parity_reduce",
        "site": "s",
        "asset": "a",
        "path": str(source),
        "allow_failure": False,
        "cadence": {"topic": "/imu", "when": "once_at_end"},
        "tasks": [
            {
                "module": "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
                "args": {
                    "event_topic": "/imu",
                    "predicate": ros_bags.PREDICATE,
                    "pre_seconds": 1.0,
                    "post_seconds": 1.0,
                },
            }
        ],
    }
    snippet_config = {
        **reduce_config,
        "name": "parity_snippet",
        "cadence": {
            "topic": "/imu",
            "when": {
                "on_event": {
                    "predicate": ros_bags.PREDICATE,
                    "debounce": {"last": 2, "unit": "second"},
                }
            },
        },
        "tasks": [
            {
                "module": "bagel_mcp.pipeline.tasks.snippet.ros2.db3",
                "lookback": {"last": 1, "unit": "second"},
                "args": {"post_seconds": 1.0},
            }
        ],
    }
    produced = []
    for config in (reduce_config, snippet_config):
        produced.extend(pipeline_base.Pipeline.build(config).run_all())
    assert len(produced) == 1 + len(ros_bags.EVENTS)
    for bag in produced:
        assert_parity(bag, ros_version=2)
        info = subprocess.run(  # noqa: S603 -- the ROS CLI on a path this test wrote
            ["ros2", "bag", "info", str(bag)],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        )
        assert info.returncode == 0, info.stderr
        assert "/imu" in info.stdout


# ---------------------------------------------------------------------------- ROS 1


@needs_ros1
def test_ros1_sample_bag_agrees() -> None:
    assert_parity("data/sample/ros1/sample.bag", ros_version=1)


@needs_ros1
@pytest.mark.parametrize("compression", [None, "bz2", "lz4"])
def test_ros1_bags_written_by_rosbags_read_natively(
    tmp_path: pathlib.Path, compression: str | None
) -> None:
    bag = ros_bags.write_ros1_imu_bag(tmp_path / "synth.bag", compression)
    assert_parity(bag, ros_version=1)
    rosbag = shutil.which("rosbag")
    assert rosbag, "rosbag CLI missing from the ROS 1 image"
    info = subprocess.run(  # noqa: S603 -- the ROS CLI on a path this test wrote
        [rosbag, "info", str(bag)], capture_output=True, text=True, check=False
    )
    assert info.returncode == 0, info.stderr
    assert "/imu" in info.stdout


@needs_ros1
def test_ros1_snippets_open_natively_and_with_the_cli(tmp_path: pathlib.Path) -> None:
    source = ros_bags.write_ros1_imu_bag(tmp_path / "synth.bag", "lz4")
    config = {
        "name": "parity_ros1_snippet",
        "site": "s",
        "asset": "a",
        "path": str(source),
        "allow_failure": False,
        "cadence": {
            "topic": "/imu",
            "when": {
                "on_event": {
                    "predicate": ros_bags.PREDICATE,
                    "debounce": {"last": 2, "unit": "second"},
                }
            },
        },
        "tasks": [
            {
                "module": "bagel_mcp.pipeline.tasks.snippet.ros1.bag",
                "lookback": {"last": 1, "unit": "second"},
                "args": {"post_seconds": 1.0},
            }
        ],
    }
    produced = pipeline_base.Pipeline.build(config).run_all()
    assert len(produced) == len(ros_bags.EVENTS)
    rosbag = shutil.which("rosbag")
    assert rosbag, "rosbag CLI missing from the ROS 1 image"
    for clip in produced:
        assert_parity(clip, ros_version=1)
        info = subprocess.run(  # noqa: S603 -- the ROS CLI on a path this test wrote
            [rosbag, "info", str(clip)], capture_output=True, text=True, check=False
        )
        assert info.returncode == 0, info.stderr
        assert "/imu" in info.stdout


@needs_ros1
def test_ros1_copy_through_the_native_writer_matches(tmp_path: pathlib.Path) -> None:
    """The escape hatch writer (native) produces what the pure-Python reader reads."""
    source = _open("data/sample/ros1/sample.bag", 1, "native")
    out = tmp_path / "native_copy.bag"
    settings.BAG_BACKEND = "native"
    try:
        with bags.open_writer(out, ros_version=1) as writer:
            for name in source.info.topic_names:
                writer.add_topic(
                    dataclasses.replace(source.info.topic(name), definition=source.definition(name))
                )
            for topic, timestamp_ns, data in source.raw_messages(
                source.info.topic_names, None, None
            ):
                writer.write(topic, timestamp_ns, data)
    finally:
        settings.BAG_BACKEND = "auto"
    assert_parity(out, ros_version=1)
