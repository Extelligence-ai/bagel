"""The bag-writing tasks (ROS 2 .db3 reduce/snippet, ROS 1 .bag snippet) on the host.

Mirrors test/pipeline/integration/test_ros2_write_paths.py, which needs rclpy to
synthesize its input and rosbag2_py to read the output; here both sides go through
the pure-Python backend, so the write paths are covered wherever the `ros` extra is.
"""

import pathlib

import pytest

from bagel_mcp import bags
from bagel_mcp.pipeline import base
from bagel_mcp.settings import settings
from test._fixtures import ros_bags

pytest.importorskip("rosbags")

PRE_SECONDS = 1.0
POST_SECONDS = 1.0
EXPECTED_EVENTS = ros_bags.event_onsets()
EXPECTED_WINDOWS = [(start - PRE_SECONDS, end + POST_SECONDS) for start, end, _ in ros_bags.EVENTS]


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def _stamps(path: pathlib.Path, ros_version: int) -> list[tuple[str, float]]:
    reader = bags.open_reader(path, ros_version)
    return [
        (topic, timestamp_ns / ros_bags.SECOND_NS)
        for topic, timestamp_ns, _ in reader.raw_messages(reader.info.topic_names, None, None)
    ]


def _in_expected_windows(timestamp: float) -> bool:
    offset = timestamp - ros_bags.EPOCH
    return any(start <= offset <= end for start, end in EXPECTED_WINDOWS)


def _snippet_config(path: pathlib.Path, module: str) -> dict:
    return {
        "name": "verify_snippet",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(path),
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
                "module": module,
                "lookback": {"last": 1, "unit": "second"},
                "args": {"post_seconds": POST_SECONDS},
            }
        ],
    }


@pytest.mark.parametrize("compression", [None, ("message", "zstd")])
def test_reduce_db3_keeps_only_event_windows(
    tmp_path: pathlib.Path, compression: tuple[str, str] | None
) -> None:
    bag = ros_bags.write_ros2_imu_bag(tmp_path / "source_bag", "sqlite3", compression)
    config = {
        "name": "verify_reduce",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(bag),
        "allow_failure": False,
        "cadence": {"topic": "/imu", "when": "once_at_end"},
        "tasks": [
            {
                "module": "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
                "args": {
                    "event_topic": "/imu",
                    "predicate": ros_bags.PREDICATE,
                    "pre_seconds": PRE_SECONDS,
                    "post_seconds": POST_SECONDS,
                },
            }
        ],
    }
    produced = base.Pipeline.build(config).run_all()

    assert len(produced) == 1
    kept = _stamps(produced[0], 2)
    assert kept, "reduced bag must not be empty"
    assert len(kept) < len(_stamps(bag, 2)), "reduction must drop data"
    assert all(_in_expected_windows(ts) for _, ts in kept), "no data outside event windows"
    assert {topic for topic, _ in kept} == {"/imu", "/status"}, "all topics preserved"
    for start, end in EXPECTED_WINDOWS:
        assert any(start <= ts - ros_bags.EPOCH <= end for _, ts in kept)
    # The output is a plain sqlite3 bag whatever the input compression was.
    output = bags.open_reader(produced[0], 2).info
    assert (output.storage_identifier, output.compression_format) == ("sqlite3", "")


def test_snippet_db3_writes_one_clip_per_event(tmp_path: pathlib.Path) -> None:
    bag = ros_bags.write_ros2_imu_bag(tmp_path / "source_bag", "sqlite3", ("file", "zstd"))
    config = _snippet_config(bag, "bagel_mcp.pipeline.tasks.snippet.ros2.db3")
    produced = base.Pipeline.build(config).run_all()

    assert len(produced) == len(EXPECTED_EVENTS), "one clip per detected event"
    for clip_dir, event_ts in zip(sorted(produced), EXPECTED_EVENTS, strict=True):
        clip = _stamps(clip_dir, 2)
        assert clip, "clip must not be empty"
        for _, ts in clip:
            assert event_ts - PRE_SECONDS <= ts <= event_ts + POST_SECONDS + 1e-6


def test_snippet_ros1_bag_writes_one_clip_per_event(tmp_path: pathlib.Path) -> None:
    bag = ros_bags.write_ros1_imu_bag(tmp_path / "source.bag", compression="bz2")
    config = _snippet_config(bag, "bagel_mcp.pipeline.tasks.snippet.ros1.bag")
    produced = base.Pipeline.build(config).run_all()

    assert len(produced) == len(EXPECTED_EVENTS), "one clip per detected event"
    for clip, event_ts in zip(sorted(produced), EXPECTED_EVENTS, strict=True):
        assert clip.suffix == ".bag"
        stamps = _stamps(clip, 1)
        assert stamps, "clip must not be empty"
        assert {topic for topic, _ in stamps} <= {"/imu", "/status"}
        for _, ts in stamps:
            assert event_ts - PRE_SECONDS <= ts <= event_ts + POST_SECONDS + 1e-6
        # The clip carries the recorded definitions and md5sums, so `rosbag` tools open it.
        info = bags.open_reader(clip, 1).info
        assert info.topic("/imu").digest == bags.open_reader(bag, 1).info.topic("/imu").digest


def test_snippet_frame_lookback_keeps_the_last_n_messages(tmp_path: pathlib.Path) -> None:
    bag = ros_bags.write_ros2_imu_bag(tmp_path / "source_bag", "sqlite3")
    config = _snippet_config(bag, "bagel_mcp.pipeline.tasks.snippet.ros2.db3")
    config["tasks"][0]["lookback"] = {"last": 5, "unit": "frame"}
    config["tasks"][0]["args"] = {"topics": ["/imu"]}
    produced = base.Pipeline.build(config).run_all()

    assert len(produced) == len(EXPECTED_EVENTS)
    for clip_dir in produced:
        assert len(_stamps(clip_dir, 2)) == 5
