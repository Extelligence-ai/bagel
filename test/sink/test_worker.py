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


def test_a_stopped_worker_ignores_new_fires() -> None:
    # Submitting happens on the transport's callback thread: it must never raise there.
    pipeline = SlowPipeline()
    worker = PipelineWorker(pipeline)
    worker.stop()
    assert worker.submit(1.0) is False
    worker.drain(timeout_seconds=0.5)
    assert pipeline.ran_at == []


def test_a_failure_stops_a_pipeline_that_does_not_allow_failures(tmp_path: pathlib.Path) -> None:
    # allow_failure: false means stop on the first failure, as a batch run does: no later
    # fire may run a broken or half-applied task again.
    pipeline = SlowPipeline(fail_at={2.0})
    pipeline.allow_failure = False
    writer = _writer(tmp_path, pipeline)
    for t in (1.0, 2.0, 3.0, 4.0):
        writer.append({"x": t})
    writer.drain(timeout_seconds=2)
    assert pipeline.ran_at == [1.0]
    assert writer.stopped
    writer.append({"x": 5.0})  # ingest keeps going; the pipeline stays stopped
    assert pipeline.ran_at == [1.0]


def test_shutdown_stops_and_joins_every_worker() -> None:
    # Registered with atexit: a worker thread still alive at interpreter exit holds a
    # thread-local DuckDB connection, and tearing that down aborts the process on Linux.
    from src.sink import worker as worker_module

    workers = [PipelineWorker(SlowPipeline()) for _ in range(3)]
    workers[0].submit(1.0)
    worker_module.shutdown_all(timeout_seconds=5)
    assert all(w.stopped and not w._thread.is_alive() for w in workers)


def test_shutdown_waits_the_drain_window_and_names_a_straggler(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A fire can outlast a few seconds (the anomaly gate's backend timeout is 10 s), so
    # exit waits the drain window, not a fixed short deadline, and says who is still
    # running if even that runs out.
    from src.sink import worker as worker_module

    monkeypatch.setattr(settings, "LIVE_PIPELINE_DRAIN_SECONDS", 0.1)
    worker = PipelineWorker(SlowPipeline(delay_seconds=0.6))
    worker.submit(1.0)
    time.sleep(0.05)  # the fire is now running
    with caplog.at_level(logging.WARNING):
        worker_module.shutdown_all()
    assert "still running" in caplog.text
    assert "slow" in caplog.text
    worker.join(5)
