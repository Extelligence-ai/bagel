"""Strip topics from a log (#223): a drop-list, and no event windowing required."""

import pathlib

import pytest
from google.protobuf.wrappers_pb2 import DoubleValue, StringValue
from mcap.reader import make_reader
from mcap_protobuf.writer import Writer as ProtobufWriter

from bagel_mcp.pipeline import base
from bagel_mcp.pipeline.tasks.reduce.mcap import ReduceMcap
from bagel_mcp.settings import settings
from bagel_mcp.source.context import SourceContext

EPOCH = 1_700_000_000.0
SECOND_NS = 1_000_000_000
MESSAGES = 20


@pytest.fixture
def robot_log(tmp_path: pathlib.Path) -> pathlib.Path:
    """/imu, /gps and /camera, one message each per second."""
    path = tmp_path / "robot.mcap"
    with open(path, "wb") as stream, ProtobufWriter(stream) as writer:
        for i in range(MESSAGES):
            ns = int((EPOCH + i) * SECOND_NS)
            writer.write_message("/imu", DoubleValue(value=-0.5 if i != 10 else -12.0), ns, ns)
            writer.write_message("/gps", StringValue(value=f"fix {i}"), ns, ns)
            writer.write_message("/camera", StringValue(value="frame"), ns, ns)
    return path


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def _run(log: pathlib.Path, args: dict) -> dict[str, int]:
    config = {
        "name": "strip",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(log),
        "allow_failure": False,
        "cadence": {"topic": "/imu", "when": "once_at_end"},
        "tasks": [{"module": "bagel_mcp.pipeline.tasks.reduce.mcap", "args": args}],
    }
    (produced,) = base.Pipeline.build(config).run_all()
    counts: dict[str, int] = {}
    with open(produced, "rb") as stream:
        for _, channel, _ in make_reader(stream).iter_messages():
            counts[channel.topic] = counts.get(channel.topic, 0) + 1
    return counts


def test_exclude_topics_keeps_everything_else_for_the_whole_log(robot_log: pathlib.Path) -> None:
    assert _run(robot_log, {"exclude_topics": ["/camera", "/gps"]}) == {"/imu": MESSAGES}


def test_keep_list_without_a_predicate(robot_log: pathlib.Path) -> None:
    assert _run(robot_log, {"topics": ["/gps"]}) == {"/gps": MESSAGES}


def test_drop_list_combines_with_event_windows(robot_log: pathlib.Path) -> None:
    counts = _run(
        robot_log,
        {
            "event_topic": "/imu",
            "predicate": "\"/imu\"['value'] < -10",
            "pre_seconds": 2.0,
            "post_seconds": 2.0,
            "exclude_topics": ["/camera"],
        },
    )
    assert set(counts) == {"/imu", "/gps"}
    assert counts["/imu"] == counts["/gps"] == 5  # t = 8..12 around the event at 10


def test_misspelled_drop_list_is_refused(robot_log: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="not in the source"):
        _run(robot_log, {"exclude_topics": ["/camera_raw"]})


def test_dropping_every_topic_is_refused(robot_log: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="empty log"):
        _run(robot_log, {"exclude_topics": ["/imu", "/gps", "/camera"]})


@pytest.mark.parametrize(
    ("args", "match"),
    [
        ({"topics": ["/imu"], "exclude_topics": ["/gps"]}, "not both"),
        ({"exclude_topics": []}, "at least one"),
        ({"event_topic": "/imu"}, "go together"),
        ({"predicate": "true"}, "go together"),
    ],
)
def test_conflicting_options_are_rejected(args: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ReduceMcap(**args)


def _counts(path: str) -> dict[str, int]:
    source = SourceContext.build(path)
    topics = source.registry.available_topics(source.factory.build())
    return {
        topic: source.dataset.to_duckdb(source.factory, source.registry, [topic])
        .aggregate("count(*)")
        .fetchone()[0]
        for topic in topics
    }


def test_db3_strip_keeps_every_message_of_the_other_topics() -> None:
    pytest.importorskip("rosbags")
    sample = "./data/sample/ros2/db3"
    before = _counts(sample)
    config = {
        "name": "strip",
        "site": "test_site",
        "asset": "test_asset",
        "path": sample,
        "allow_failure": False,
        "cadence": {"topic": "AAA", "when": "once_at_end"},
        "tasks": [
            {
                "module": "bagel_mcp.pipeline.tasks.reduce.ros2.db3",
                "args": {"exclude_topics": ["BBB", "CCC"]},
            }
        ],
    }
    (produced,) = base.Pipeline.build(config).run_all()
    # This bag's metadata starts after its first messages: whole-log mode must not trim.
    assert _counts(str(produced)) == {
        topic: count for topic, count in before.items() if topic not in {"BBB", "CCC"}
    }
