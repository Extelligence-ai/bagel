"""End-to-end tests for MCAP as a first-class, middleware-independent format.

Runs WITHOUT ROS: writes a protobuf-encoded MCAP file with `mcap_protobuf`, then
drives resolve -> describe -> schema -> SQL -> preview -> reduce through the generic
`mcap` modules. Also proves a ROS2-profile MCAP bag reads through the same generic
path (the mcap_ros2 decoder is pure Python).
"""

import pathlib

import pytest
from google.protobuf.wrappers_pb2 import DoubleValue
from mcap.reader import make_reader
from mcap_protobuf.writer import Writer as ProtobufWriter

import server
from settings import settings
from src.di import module
from src.di.types import data_source
from src.pipeline import base

EPOCH = 1_700_000_000.0
SECOND_NS = 1_000_000_000

RATE_HZ = 4
DURATION_SECONDS = 12.0
# (start offset, end offset, accel): two hard decelerations below -10.
EVENTS = ((4.0, 5.0, -13.0), (8.0, 8.75, -11.5))
PRE_SECONDS, POST_SECONDS = 1.0, 1.0

ROS2_SAMPLE = "./data/sample/ros2/mcap"


def _accel_at(offset: float) -> float:
    for start, end, value in EVENTS:
        if start <= offset <= end:
            return value
    return -0.5


def _write_protobuf_mcap(path: pathlib.Path) -> pathlib.Path:
    with open(path, "wb") as stream, ProtobufWriter(stream) as writer:
        for i in range(int(DURATION_SECONDS * RATE_HZ)):
            offset = i / RATE_HZ
            timestamp_ns = int((EPOCH + offset) * SECOND_NS)
            writer.write_message(
                topic="/accel",
                message=DoubleValue(value=_accel_at(offset)),
                log_time=timestamp_ns,
                publish_time=timestamp_ns,
            )
    return path


@pytest.fixture
def protobuf_mcap(tmp_path: pathlib.Path) -> pathlib.Path:
    return _write_protobuf_mcap(tmp_path / "flight.mcap")


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def test_bare_mcap_file_resolves_to_first_class_mcap(protobuf_mcap: pathlib.Path) -> None:
    assert data_source.resolve(str(protobuf_mcap)) == data_source.DataSource.MCAP


def test_source_factory_reads_metadata_without_ros(protobuf_mcap: pathlib.Path) -> None:
    factory = module.provide("src.source.mcap", {"path": str(protobuf_mcap)})
    metadata = factory.metadata
    assert metadata["total_message_count"] == int(DURATION_SECONDS * RATE_HZ)
    assert metadata["duration_seconds"] == pytest.approx(DURATION_SECONDS - 1 / RATE_HZ)
    (topic_info,) = metadata["topic_information"]
    assert topic_info["name"] == "/accel"
    assert topic_info["type"] == "google.protobuf.DoubleValue"
    assert topic_info["schema_encoding"] == "protobuf"


def test_topic_registry_builds_struct_from_protobuf_schema(
    protobuf_mcap: pathlib.Path,
) -> None:
    factory = module.provide("src.source.mcap", {"path": str(protobuf_mcap)})
    registry = module.provide("src.topic.mcap", {})
    bag = factory.build()

    assert registry.available_topics(bag) == ["/accel"]
    assert registry.native_type_name("/accel", bag) == "google.protobuf.DoubleValue"
    assert registry.message_count("/accel", bag) == int(DURATION_SECONDS * RATE_HZ)

    struct = registry.struct("/accel", bag)
    assert struct.field("value").type == "double"
    assert isinstance(registry.describe("/accel", bag), str)


def test_query_messages_over_generic_mcap(protobuf_mcap: pathlib.Path) -> None:
    rows = server.query_messages(
        path=str(protobuf_mcap),
        sql_statement='SELECT MIN("/accel"[\'value\']) AS min_accel FROM "/accel"',
        topic="/accel",
    )
    assert rows == [{"min_accel": -13.0}]


def test_preview_and_reduce_generic_mcap_without_ros(protobuf_mcap: pathlib.Path) -> None:
    predicate = "\"/accel\"['value'] < -10"

    preview = server.preview_pipeline(
        path=str(protobuf_mcap),
        event_topic="/accel",
        predicate=predicate,
        pre_seconds=PRE_SECONDS,
        post_seconds=POST_SECONDS,
        debounce_seconds=2.0,
    )
    assert preview["event_count"] == len(EVENTS)

    config = {
        "name": "reduce_protobuf_mcap",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(protobuf_mcap),
        "allow_failure": False,
        "cadence": {"topic": "/accel", "when": "once_at_end"},
        "tasks": [
            {
                "module": "src.pipeline.tasks.reduce.mcap",
                "args": {
                    "event_topic": "/accel",
                    "predicate": predicate,
                    "pre_seconds": PRE_SECONDS,
                    "post_seconds": POST_SECONDS,
                },
            }
        ],
    }
    produced = base.Pipeline.build(config).run_all()
    assert len(produced) == 1

    windows = [(start - PRE_SECONDS, end + POST_SECONDS) for start, end, _ in EVENTS]
    kept = []
    with open(produced[0], "rb") as stream:
        for schema, channel, message in make_reader(stream).iter_messages():
            assert channel.topic == "/accel"
            assert schema.name == "google.protobuf.DoubleValue"
            kept.append(message.log_time / SECOND_NS - EPOCH)

    assert kept, "reduced mcap must not be empty"
    assert len(kept) < int(DURATION_SECONDS * RATE_HZ), "reduction must drop data"
    assert all(any(start <= ts <= end for start, end in windows) for ts in kept)


def test_ros2_profile_sample_reads_through_generic_path() -> None:
    assert data_source.resolve(ROS2_SAMPLE) == data_source.DataSource.MCAP

    factory = module.provide("src.source.mcap", {"path": ROS2_SAMPLE})
    registry = module.provide("src.topic.mcap", {})
    bag = factory.build()

    topics = registry.available_topics(bag)
    assert "/fluid_pressure" in topics
    assert registry.native_type_name("/fluid_pressure", bag) == "sensor_msgs/msg/FluidPressure"

    # ros2msg schema parsed into an Arrow struct without any ROS installation.
    struct = registry.struct("/fluid_pressure", bag)
    assert "fluid_pressure" in struct.names

    rows = server.query_messages(
        path=ROS2_SAMPLE,
        sql_statement=(
            "SELECT COUNT(*) AS n, AVG(\"/fluid_pressure\"['fluid_pressure']) AS avg_pressure "
            'FROM "/fluid_pressure"'
        ),
        topic="/fluid_pressure",
    )
    assert rows[0]["n"] > 0
    assert rows[0]["avg_pressure"] is not None


def test_reduce_task_back_compat_alias_registers_same_class() -> None:
    from src.pipeline.tasks.reduce import mcap as new_module
    from src.pipeline.tasks.reduce.ros2 import mcap as old_module

    old_module.register()
    new_module.register()
    assert (
        module.global_registry["src.pipeline.tasks.reduce.ros2.mcap"]
        is module.global_registry["src.pipeline.tasks.reduce.mcap"]
    )
