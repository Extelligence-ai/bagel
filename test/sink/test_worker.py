"""Live pipelines run on a worker thread, never on the transport's ingest thread."""

import logging
import pathlib
import threading
import time

import pyarrow as pa
import pytest

from settings import settings
from src.pipeline.base import Cadence, Frequency, Unit
from src.pipeline.results import RunSummary
from src.sink.buffer import TopicBufferWriter
from src.sink.worker import PipelineWorker

TOPIC = "/x"


class SlowPipeline:
    """Duck-typed pipeline: records where and when each fire ran."""

    def __init__(self, delay_seconds: float = 0.0, fail_at: set[float] | None = None) -> None:
        self.name = "slow"
        self.cadence = Cadence(topic=TOPIC, when=Frequency(every=1, unit=Unit.FRAME))
        self.summary = RunSummary()
        self.ran_at: list[float] = []
        self.threads: set[int] = set()
        self._delay = delay_seconds
        self._fail_at = fail_at or set()

    def run_at(self, asof_seconds: float) -> None:
        self.threads.add(threading.get_ident())
        time.sleep(self._delay)
        if asof_seconds in self._fail_at:
            raise RuntimeError(f"boom at {asof_seconds}")
        self.ran_at.append(asof_seconds)


def _writer(tmp_path: pathlib.Path, pipeline: object) -> TopicBufferWriter:
    return TopicBufferWriter(
        path=tmp_path,
        topic=TOPIC,
        type_name="t",
        definition="float64 x",
        struct=pa.struct([pa.field("x", pa.float64())]),
        buffer_size_bytes=None,
        overwrite=False,
        pipeline=pipeline,
        extract_timestamp=lambda msg: msg["x"],
    )


def test_fires_run_off_the_ingest_thread_in_order(tmp_path: pathlib.Path) -> None:
    pipeline = SlowPipeline()
    writer = _writer(tmp_path, pipeline)
    for t in (1.0, 2.0, 3.0):
        writer.append({"x": t})
    writer.drain()
    assert pipeline.ran_at == [1.0, 2.0, 3.0]
    assert threading.get_ident() not in pipeline.threads
    writer.stop()


def test_a_slow_pipeline_does_not_stall_ingest(tmp_path: pathlib.Path) -> None:
    # A decision backend taking 0.3 s per window: ingest must not wait on it.
    pipeline = SlowPipeline(delay_seconds=0.3)
    writer = _writer(tmp_path, pipeline)
    started = time.monotonic()
    for t in (1.0, 2.0, 3.0, 4.0):
        writer.append({"x": t})
    assert time.monotonic() - started < 0.25
    writer.drain()
    assert pipeline.ran_at == [1.0, 2.0, 3.0, 4.0]
    writer.stop()


def test_a_full_queue_drops_the_oldest_fires_and_counts_them(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(settings, "LIVE_PIPELINE_MAX_PENDING", 2)
    gate = threading.Event()
    pipeline = SlowPipeline()
    original = pipeline.run_at

    def blocked_run_at(asof_seconds: float) -> None:
        gate.wait(5)
        original(asof_seconds)

    pipeline.run_at = blocked_run_at
    writer = _writer(tmp_path, pipeline)
    writer.append({"x": 1.0})  # taken by the worker, which then blocks
    time.sleep(0.1)
    with caplog.at_level(logging.WARNING):
        for t in (2.0, 3.0, 4.0, 5.0):
            writer.append({"x": t})
    gate.set()
    writer.drain()
    # 2.0 and 3.0 were dropped to keep the newest two: the worker stays current.
    assert pipeline.ran_at == [1.0, 4.0, 5.0]
    assert pipeline.summary.dropped == 2
    assert "dropped" in caplog.text
    writer.stop()


def test_stale_fires_are_dropped_rather_than_run_on_rotated_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "LIVE_PIPELINE_MAX_LAG_SECONDS", 0.2)
    gate = threading.Event()
    pipeline = SlowPipeline()
    original = pipeline.run_at

    def blocked_run_at(asof_seconds: float) -> None:
        gate.wait(5)
        original(asof_seconds)

    pipeline.run_at = blocked_run_at
    worker = PipelineWorker(pipeline)
    worker.submit(1.0)
    time.sleep(0.05)
    worker.submit(2.0)  # waits behind 1.0 for longer than the allowed lag
    time.sleep(0.4)
    gate.set()
    worker.drain()
    assert pipeline.ran_at == [1.0]
    assert pipeline.summary.dropped == 1
    worker.stop()


def test_a_failing_fire_does_not_stop_the_worker(tmp_path: pathlib.Path) -> None:
    pipeline = SlowPipeline(fail_at={2.0})
    writer = _writer(tmp_path, pipeline)
    for t in (1.0, 2.0, 3.0):
        writer.append({"x": t})
    writer.drain()
    assert pipeline.ran_at == [1.0, 3.0]
    writer.stop()


def test_drain_gives_up_after_its_timeout() -> None:
    pipeline = SlowPipeline(delay_seconds=1.0)
    worker = PipelineWorker(pipeline)
    worker.submit(1.0)
    started = time.monotonic()
    assert worker.drain(timeout_seconds=0.1) is False
    assert time.monotonic() - started < 0.5
    worker.stop()


def test_a_stopped_worker_refuses_new_fires() -> None:
    worker = PipelineWorker(SlowPipeline())
    worker.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        worker.submit(1.0)
