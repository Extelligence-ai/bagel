"""End-to-end verification of the ROS2 reduce/snippet write paths.

These tests require a ROS2 environment (rclpy, rosbag2_py, the mcap python libs) and
are skipped elsewhere -- run them inside a bagel ros2-* container:

    docker compose run --rm -v "$PWD:/home/ubuntu/work" ros2-jazzy bash -c \
      "cd /home/ubuntu/work && UV_PROJECT_ENVIRONMENT=/home/ubuntu/runtime/.venv \
       uv run --no-sync pytest test/pipeline/integration -v"

Each test synthesizes a small bag with real numeric IMU telemetry (two hard
decelerations below -10), runs the actual pipeline (`Pipeline.build().run_all()`),
and asserts on the *written output bag* -- proving the write layer end to end.
"""

import pathlib

import pytest

rosbag2_py = pytest.importorskip("rosbag2_py")

from settings import settings  # noqa: E402
from src.pipeline import base  # noqa: E402

from . import synth  # noqa: E402

SECOND_NS = 1_000_000_000

PREDICATE = "\"/imu\"['linear_acceleration']['x'] < -10"
PRE_SECONDS = 1.0
POST_SECONDS = 1.0

# Ground truth from synth: events at EPOCH+4 and EPOCH+8 -> kept windows [3,6] and [7,9.75].
EXPECTED_EVENTS = synth.event_onsets()
EXPECTED_WINDOWS = [(start - PRE_SECONDS, end + POST_SECONDS) for start, end, _ in synth.EVENTS]


def _read_messages(bag_dir: pathlib.Path, storage_id: str) -> list[tuple[str, float]]:
    """Read (topic, timestamp_seconds) pairs from an output bag via rosbag2."""
    reader = rosbag2_py.SequentialReader()
    reader.open(
        rosbag2_py.StorageOptions(uri=str(bag_dir), storage_id=storage_id),
        rosbag2_py.ConverterOptions("", ""),
    )
    messages = []
    while reader.has_next():
        topic, _, nanoseconds = reader.read_next()
        messages.append((topic, nanoseconds / SECOND_NS))
    return messages


def _in_expected_windows(timestamp: float) -> bool:
    offset = timestamp - synth.EPOCH
    return any(start <= offset <= end for start, end in EXPECTED_WINDOWS)


def _reduce_config(bag_path: pathlib.Path, module: str) -> dict:
    return {
        "name": "verify_reduce",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(bag_path),
        "allow_failure": False,
        "cadence": {"topic": "/imu", "when": "once_at_end"},
        "tasks": [
            {
                "module": module,
                "args": {
                    "event_topic": "/imu",
                    "predicate": PREDICATE,
                    "pre_seconds": PRE_SECONDS,
                    "post_seconds": POST_SECONDS,
                },
            }
        ],
    }


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def test_reduce_db3_keeps_only_event_windows(tmp_path: pathlib.Path) -> None:
    bag = synth.write_imu_bag(tmp_path / "source_bag", "sqlite3")

    pipeline = base.Pipeline.build(_reduce_config(bag, "src.pipeline.tasks.reduce.ros2.db3"))
    produced = pipeline.run_all()

    assert len(produced) == 1
    source_count = len(_read_messages(bag, "sqlite3"))
    kept = _read_messages(produced[0], "sqlite3")

    assert kept, "reduced bag must not be empty"
    assert len(kept) < source_count, "reduction must drop data"
    assert all(_in_expected_windows(ts) for _, ts in kept), "no data outside event windows"
    assert {topic for topic, _ in kept} == {"/imu", "/status"}, "all topics preserved"
    # Both events' windows are represented.
    for start, end in EXPECTED_WINDOWS:
        assert any(start <= ts - synth.EPOCH <= end for _, ts in kept)


def test_snippet_db3_writes_one_clip_per_event(tmp_path: pathlib.Path) -> None:
    bag = synth.write_imu_bag(tmp_path / "source_bag", "sqlite3")

    config = {
        "name": "verify_snippet",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(bag),
        "allow_failure": False,
        "cadence": {
            "topic": "/imu",
            "when": {
                "on_event": {
                    "predicate": PREDICATE,
                    "debounce": {"last": 2, "unit": "second"},
                }
            },
        },
        "tasks": [
            {
                "module": "src.pipeline.tasks.snippet.ros2.db3",
                "lookback": {"last": 1, "unit": "second"},
                "args": {"post_seconds": POST_SECONDS},
            }
        ],
    }
    produced = base.Pipeline.build(config).run_all()

    assert len(produced) == len(EXPECTED_EVENTS), "one clip per detected event"
    for clip_dir, event_ts in zip(sorted(produced), EXPECTED_EVENTS, strict=True):
        clip = _read_messages(clip_dir, "sqlite3")
        assert clip, "clip must not be empty"
        for _, ts in clip:
            assert event_ts - PRE_SECONDS <= ts <= event_ts + POST_SECONDS + 1e-6


def test_reduce_mcap_raw_passthrough(tmp_path: pathlib.Path) -> None:
    from mcap.reader import make_reader

    bag = synth.write_imu_bag(tmp_path / "source_bag", "mcap")

    pipeline = base.Pipeline.build(_reduce_config(bag, "src.pipeline.tasks.reduce.mcap"))
    produced = pipeline.run_all()

    assert len(produced) == 1
    output_file = produced[0]
    assert output_file.suffix == ".mcap"

    kept, topics, schemas = [], set(), set()
    with open(output_file, "rb") as stream:
        reader = make_reader(stream)
        for schema, channel, message in reader.iter_messages():
            kept.append((channel.topic, message.log_time / SECOND_NS))
            topics.add(channel.topic)
            schemas.add(schema.name)

    assert kept, "reduced mcap must not be empty"
    assert all(_in_expected_windows(ts) for _, ts in kept), "no data outside event windows"
    assert topics == {"/imu", "/status"}
    assert schemas == {"sensor_msgs/msg/Imu", "std_msgs/msg/String"}, "schemas copied verbatim"


def test_snippet_mcap_writes_one_clip_per_event(tmp_path: pathlib.Path) -> None:
    from mcap.reader import make_reader

    bag = synth.write_imu_bag(tmp_path / "source_bag", "mcap")

    config = {
        "name": "verify_snippet_mcap",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(bag),
        "allow_failure": False,
        "cadence": {
            "topic": "/imu",
            "when": {
                "on_event": {
                    "predicate": PREDICATE,
                    "debounce": {"last": 2, "unit": "second"},
                }
            },
        },
        "tasks": [
            {
                "module": "src.pipeline.tasks.snippet.mcap",
                "lookback": {"last": 1, "unit": "second"},
                "args": {"post_seconds": POST_SECONDS},
            }
        ],
    }
    produced = base.Pipeline.build(config).run_all()

    assert len(produced) == len(EXPECTED_EVENTS), "one clip per detected event"
    for clip_path, event_ts in zip(sorted(produced), EXPECTED_EVENTS, strict=True):
        with open(clip_path, "rb") as stream:
            timestamps = [
                message.log_time / SECOND_NS
                for _, _, message in make_reader(stream).iter_messages()
            ]
        assert timestamps, "clip must not be empty"
        for ts in timestamps:
            assert event_ts - PRE_SECONDS <= ts <= event_ts + POST_SECONDS + 1e-6


def test_preview_reports_ground_truth_events(tmp_path: pathlib.Path) -> None:
    import server

    bag = synth.write_imu_bag(tmp_path / "source_bag", "sqlite3")
    result = server.preview_pipeline(
        path=str(bag),
        event_topic="/imu",
        predicate=PREDICATE,
        pre_seconds=PRE_SECONDS,
        post_seconds=POST_SECONDS,
        debounce_seconds=2.0,
    )
    assert result["event_count"] == len(EXPECTED_EVENTS)
    assert [round(e - synth.EPOCH, 3) for e in result["events"]] == [
        start for start, _, _ in synth.EVENTS
    ]
