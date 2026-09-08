"""Unit tests for the file-backed topic buffer (issue #72)."""

import json
import pathlib
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest

from src.pipeline.base import Cadence, Frequency, Lookback, OnceAtEnd, OnEvent, Unit
from src.sink.base import TopicSink
from src.sink.buffer import TopicBufferReader, TopicBufferWriter, _artifact_paths

TOPIC = "/imu"
STRUCT = pa.struct([pa.field("x", pa.float64()), pa.field("note", pa.string())])
DEFINITION = "float64 x\nstring note"


def make_writer(path: pathlib.Path, **overrides: object) -> TopicBufferWriter:
    params = {
        "path": path,
        "topic": TOPIC,
        "type_name": "test/Imu",
        "definition": DEFINITION,
        "struct": STRUCT,
        "buffer_size_bytes": None,
        "overwrite": False,
        "pipeline": None,
        "extract_timestamp": lambda msg: msg["x"],
    }
    params.update(overrides)
    return TopicBufferWriter(**params)


class PipelineStub:
    """Duck-typed stand-in: the writer only touches `.cadence` and `.run_at`."""

    def __init__(self, when: object) -> None:
        self.name = "stub"
        self.cadence = Cadence(topic=TOPIC, when=when)
        self.ran_at: list[float] = []

    def run_at(self, asof_seconds: float) -> None:
        self.ran_at.append(asof_seconds)


def test_round_trip_messages_and_self_description(tmp_path: pathlib.Path) -> None:
    writer = make_writer(tmp_path)
    for i in range(5):
        writer.append({"x": float(i), "note": f"msg {i}"})

    assert writer.message_count == 5
    assert writer.last_timestamp_seconds == 4.0

    reader = TopicBufferReader(tmp_path, TOPIC)
    messages = list(reader.messages())
    assert [ts for ts, _ in messages] == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert messages[2][1] == {"x": 2.0, "note": "msg 2"}

    # Self-describing: metadata, definition, and schema survive the disk round trip.
    assert reader.topic == TOPIC
    assert reader.type_name == "test/Imu"
    assert reader.definition == DEFINITION
    assert reader.struct == STRUCT


def test_eviction_rolls_current_into_overflow(tmp_path: pathlib.Path) -> None:
    one_line = len(
        json.dumps({"timestamp_seconds": 0.0, TOPIC: {"x": 0.0, "note": "msg 0"}}) + "\n"
    )
    writer = make_writer(tmp_path, buffer_size_bytes=3 * one_line)
    for i in range(8):
        writer.append({"x": float(i), "note": f"msg {i}"})

    paths = _artifact_paths(tmp_path, TOPIC)
    assert paths["overflow_data_file"].exists()
    assert paths["current_data_file"].exists()

    # The reader honors the byte budget: only the newest messages within
    # buffer_size_bytes come back, oldest evicted first.
    timestamps = [ts for ts, _ in TopicBufferReader(tmp_path, TOPIC).messages()]
    assert len(timestamps) == 3
    assert timestamps == [5.0, 6.0, 7.0]


def test_overwrite_clears_and_false_preserves(tmp_path: pathlib.Path) -> None:
    writer = make_writer(tmp_path)
    writer.append({"x": 1.0, "note": "old"})

    # overwrite=False: a reconnecting writer appends to the existing buffer.
    again = make_writer(tmp_path)
    again.append({"x": 2.0, "note": "new"})
    assert len(list(TopicBufferReader(tmp_path, TOPIC).messages())) == 2

    # overwrite=True: the buffer starts fresh.
    fresh = make_writer(tmp_path, overwrite=True)
    fresh.append({"x": 3.0, "note": "fresh"})
    messages = list(TopicBufferReader(tmp_path, TOPIC).messages())
    assert [msg["note"] for _, msg in messages] == ["fresh"]


def test_frame_cadence_fires_every_nth_message(tmp_path: pathlib.Path) -> None:
    pipeline = PipelineStub(Frequency(every=2, unit=Unit.FRAME))
    writer = make_writer(tmp_path, pipeline=pipeline)
    for i in range(5):
        writer.append({"x": float(i), "note": ""})

    # Fires on the first message (never ran), then on every 2nd frame.
    assert pipeline.ran_at == [0.0, 2.0, 4.0]


def test_second_cadence_fires_by_elapsed_time(tmp_path: pathlib.Path) -> None:
    pipeline = PipelineStub(Frequency(every=10, unit=Unit.SECOND))
    writer = make_writer(tmp_path, pipeline=pipeline)
    for t in [0.0, 5.0, 10.0, 15.0, 21.0]:
        writer.append({"x": t, "note": ""})

    assert pipeline.ran_at == [0.0, 10.0, 21.0]


def test_on_event_forward_window_flushes_at_close(tmp_path: pathlib.Path) -> None:
    when = OnEvent(
        predicate=f"\"{TOPIC}\"['x'] < -10",
        forward=Lookback(last=5, unit=Unit.SECOND),
    )
    pipeline = PipelineStub(when)
    writer = make_writer(tmp_path, pipeline=pipeline)

    writer.append({"x": -20.0, "note": "hard brake"})  # rising edge at t=-20.0? no: ts=x
    # The event is pending its 5s forward window, so nothing has run yet.
    assert pipeline.ran_at == []

    # Stream ends before the post-window elapsed: flush fires it best-effort.
    writer.flush_pending_events()
    assert pipeline.ran_at == [-20.0]


def test_concurrent_writers_do_not_corrupt_the_buffer(tmp_path: pathlib.Path) -> None:
    writers = [make_writer(tmp_path), make_writer(tmp_path)]

    def append_many(writer: TopicBufferWriter, offset: int) -> None:
        for i in range(50):
            writer.append({"x": float(offset + i), "note": "concurrent"})

    threads = [
        threading.Thread(target=append_many, args=(writer, n * 1000))
        for n, writer in enumerate(writers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Every line parses and every message arrived: the file locks did their job.
    messages = list(TopicBufferReader(tmp_path, TOPIC).messages())
    assert len(messages) == 100
    expected = {float(v) for n in range(2) for v in range(n * 1000, n * 1000 + 50)}
    assert {ts for ts, _ in messages} == expected


def test_reader_requires_an_existing_buffer(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError):
        TopicBufferReader(tmp_path, "/never_written")


def test_once_at_end_never_runs_from_append(tmp_path: pathlib.Path) -> None:
    pipeline = PipelineStub(OnceAtEnd())
    writer = make_writer(tmp_path, pipeline=pipeline)
    for timestamp in (1.0, 2.0):
        writer.append({"x": timestamp, "note": "end-only"})
    assert pipeline.ran_at == []
    assert writer.last_timestamp_seconds == 2.0


@pytest.mark.parametrize("timestamps", [[], [1.0], [1.0, 2.0]])
def test_once_at_end_runs_only_once_when_sink_closes(
    tmp_path: pathlib.Path, timestamps: list[float]
) -> None:
    pipeline = PipelineStub(OnceAtEnd())
    writer = make_writer(tmp_path, pipeline=pipeline)
    for timestamp in timestamps:
        writer.append({"x": timestamp, "note": "end-only"})
    sink = SimpleNamespace(
        _is_singleton_initialized=True,
        host="test",
        port=12345,
        _buffers={TOPIC: writer},
        pause=Mock(),
        _disconnect=Mock(),
    )
    TopicSink.close(sink)
    TopicSink.close(sink)
    assert pipeline.ran_at == timestamps[-1:]
