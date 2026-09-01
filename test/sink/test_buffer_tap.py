"""The fleet tap seam on the live buffer: non-blocking, failure-isolated."""

import pathlib

import pyarrow as pa
import pytest

from src.sink.buffer import TopicBufferWriter

TOPIC = "/imu"
STRUCT = pa.struct([pa.field("x", pa.float64())])
DEFINITION = "float64 x"


@pytest.fixture()
def writer(tmp_path: pathlib.Path) -> TopicBufferWriter:
    return TopicBufferWriter(
        path=tmp_path,
        topic=TOPIC,
        type_name="test/Imu",
        definition=DEFINITION,
        struct=STRUCT,
        buffer_size_bytes=None,
        overwrite=False,
        pipeline=None,
        extract_timestamp=lambda msg: msg["timestamp_seconds"],
    )


def test_append_without_tap_unchanged(writer: TopicBufferWriter) -> None:
    writer.append({"x": 1.0, "timestamp_seconds": 5.0})
    assert writer.message_count == 1


def test_tap_receives_topic_timestamp_and_raw_msg(writer: TopicBufferWriter) -> None:
    seen = []
    writer.set_tap(lambda topic, t, msg: seen.append((topic, t, msg)))
    writer.append({"x": 2.5, "timestamp_seconds": 7.0})
    assert seen == [("/imu", 7.0, {"x": 2.5, "timestamp_seconds": 7.0})]


def test_tap_exception_never_breaks_append(writer: TopicBufferWriter) -> None:
    def boom(topic: str, t: float, msg: dict) -> None:
        raise RuntimeError("tap exploded")

    writer.set_tap(boom)
    writer.append({"x": 1.0, "timestamp_seconds": 1.0})  # must not raise
    assert writer.message_count == 1


def test_tap_can_be_cleared(writer: TopicBufferWriter) -> None:
    seen = []
    writer.set_tap(lambda *a: seen.append(a))
    writer.set_tap(None)
    writer.append({"x": 1.0, "timestamp_seconds": 1.0})
    assert seen == []
