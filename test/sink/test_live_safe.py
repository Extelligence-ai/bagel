"""Decision gates attach to live subscriptions: pipelines run off the ingest thread."""

import itertools
import pathlib

import pyarrow as pa
import pytest

from settings import settings
from src.pipeline import base as pipeline_base
from src.sink import base
from src.sink.buffer import TopicBufferWriter
from test._fixtures.fault_log import write_fault_log

_port_counter = itertools.count(19500)


class _FakeSink(base.TopicSink):
    def _connect(self) -> None:
        pass

    def _disconnect(self) -> None:
        pass

    def _available_topics(self) -> list[str]:
        return ["/a", "/b"]

    def _type_name(self, topic: str) -> str:
        return "test/type"

    def _definition(self, topic: str) -> str:
        return "float64 x"

    def _struct(self, topic: str) -> pa.StructType:
        return pa.struct([pa.field("x", pa.float64())])

    def _subscribe(self, writer: TopicBufferWriter) -> None:
        pass

    def _unsubscribe(self, writer: TopicBufferWriter) -> None:
        pass


@pytest.fixture
def sink(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> _FakeSink:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    return _FakeSink("localhost", next(_port_counter))


def _config(cadence_topic: str) -> dict:
    return {
        "name": "p",
        "site": "s",
        "asset": "a",
        "allow_failure": False,
        "cadence": {"topic": cadence_topic, "when": {"every": 10, "unit": "second"}},
        "gates": [
            {
                "module": "src.pipeline.gates.anomaly",
                "lookback": {"last": 10, "unit": "second"},
                "args": {"anomalies": {"overcurrent": "too much current"}},
            }
        ],
        "tasks": [{"module": "src.pipeline.tasks.write_annotations"}],
    }


def test_a_live_topic_accepts_an_anomaly_pipeline(sink: _FakeSink, tmp_path: pathlib.Path) -> None:
    log = write_fault_log(tmp_path / "log.mcap", duration_seconds=30)
    pipeline = pipeline_base.Pipeline.build({**_config("/motor/current"), "path": str(log)})
    sink.subscribe("/a", pipeline=pipeline)
    assert sink._buffers["/a"].pipeline is pipeline
    sink.close()


def test_subscribe_with_pipeline_accepts_an_anomaly_pipeline(sink: _FakeSink) -> None:
    from src.sink import startup

    assert startup.subscribe_with_pipeline(sink, ["/a", "/b"], _config("/b")) == ["/a", "/b"]
    assert sink._buffers["/b"].pipeline is not None
    sink.close()


def test_resubscribing_a_topic_stops_the_replaced_pipeline_worker(sink: _FakeSink) -> None:
    from src.sink import startup

    startup.subscribe_with_pipeline(sink, ["/b"], _config("/b"))
    old = sink._buffers["/b"]
    startup.subscribe_with_pipeline(sink, ["/b"], _config("/b"), overwrite=True)
    assert old.stopped
    assert not sink._buffers["/b"].stopped
    sink.close()


class _SlowPipeline:
    """Duck-typed live pipeline whose single fire takes a while."""

    def __init__(self, seconds: float) -> None:
        from src.pipeline.base import Cadence, Frequency, Unit
        from src.pipeline.results import RunSummary

        self.name = "slow"
        self.cadence = Cadence(topic="/a", when=Frequency(every=1, unit=Unit.FRAME))
        self.summary = RunSummary()
        self.finished: list[float] = []
        self._seconds = seconds

    def run_at(self, asof_seconds: float) -> None:
        import time

        time.sleep(self._seconds)
        self.finished.append(asof_seconds)


def test_overwrite_waits_for_the_replaced_pipelines_fire_to_finish(sink: _FakeSink) -> None:
    # The replacement writer resets the topic's buffer files; the old pipeline must not
    # still be reading them.
    import time

    old = _SlowPipeline(0.4)
    sink.subscribe("/a", pipeline=old, buffer_size_bytes=None)
    sink._buffers["/a"].append({"x": 1.0})
    time.sleep(0.05)  # the fire is now running
    replaced = sink._buffers["/a"]
    sink.subscribe("/a", overwrite=True, buffer_size_bytes=None)
    assert len(old.finished) == 1
    assert not replaced._worker._thread.is_alive()
    sink.close()


def test_overwrite_refuses_while_the_replaced_fire_is_still_running(
    sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    # If the old fire outlasts the drain window, resetting its buffer files under it
    # would corrupt what it reads: refuse the overwrite instead.
    import time

    monkeypatch.setattr(settings, "LIVE_PIPELINE_DRAIN_SECONDS", 0.1)
    old = _SlowPipeline(0.6)
    sink.subscribe("/a", pipeline=old, buffer_size_bytes=None)
    replaced = sink._buffers["/a"]
    replaced.append({"x": 1.0})
    time.sleep(0.05)  # the fire is now running
    with pytest.raises(base.PipelineStillRunningError, match="/a"):
        sink.subscribe("/a", overwrite=True, buffer_size_bytes=None)
    assert sink._buffers["/a"] is replaced  # nothing was reset under the old fire
    replaced.join(5)
    sink.close()


def test_a_failed_transport_subscribe_leaves_no_writer_or_worker(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _BrokenSink(_FakeSink):
        def _subscribe(self, writer: TopicBufferWriter) -> None:
            raise RuntimeError("broker refused the subscription")

    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
    broken = _BrokenSink("localhost", next(_port_counter))
    pipeline = _SlowPipeline(0.0)
    with pytest.raises(RuntimeError, match="refused"):
        broken.subscribe("/a", pipeline=pipeline, buffer_size_bytes=None)
    assert "/a" not in broken._buffers
    import threading

    assert not [t for t in threading.enumerate() if t.name == "pipeline:slow" and t.is_alive()]
    broken.close()


@pytest.mark.parametrize(
    "task",
    [
        {"module": "src.pipeline.tasks.snippet.mcap"},
        {"module": "src.pipeline.tasks.snippet.ros1.bag"},
        {"module": "src.pipeline.tasks.cloudini.compress_pointcloud"},
        {
            "module": "src.pipeline.tasks.reduce.mcap",
            "args": {"event_topic": "/b", "predicate": "x > 1", "pre_seconds": 5},
        },
    ],
)
def test_a_live_pipeline_refuses_tasks_that_need_a_recorded_log(
    sink: _FakeSink, task: dict
) -> None:
    # The live sink buffer is not a bag file: these tasks would fail on every fire (and
    # with allow_failure false, stop the pipeline). Refuse them before subscribing.
    from src.sink import startup

    config = _config("/b")
    config["tasks"] = [task, *config["tasks"]]
    with pytest.raises(ValueError, match=r"recorded log.*write_topics_to_file"):
        startup.subscribe_with_pipeline(sink, ["/a", "/b"], config)
    assert not sink._buffers
    sink.close()


def test_a_live_pipeline_can_slice_the_window_to_parquet(sink: _FakeSink) -> None:
    from src.sink import startup

    config = _config("/b")
    config["tasks"] = [
        {
            "module": "src.pipeline.tasks.write_topics_to_file",
            "lookback": {"last": 10, "unit": "second"},
            "args": {"topics": None, "output_format": "parquet"},
        },
        *config["tasks"],
    ]
    assert startup.subscribe_with_pipeline(sink, ["/a", "/b"], config) == ["/a", "/b"]
    sink.close()


def test_unsubscribe_runs_queued_fires_then_stops_only_that_topic(sink: _FakeSink) -> None:
    pipeline = _SlowPipeline(0.1)
    sink.subscribe("/a", pipeline=pipeline, buffer_size_bytes=None)
    sink.subscribe("/b", buffer_size_bytes=None)
    writer = sink._buffers["/a"]
    writer.append({"x": 1.0})
    sink.unsubscribe(["/a"])
    assert pipeline.finished == [pytest.approx(pipeline.finished[0])]  # the queued fire ran
    assert writer.stopped
    assert sink.subscribed_topics == ["/b"]
    assert writer._data_directory.exists()  # the recorded buffer stays for analysis
    sink.close()


def test_unsubscribe_an_unknown_topic_changes_nothing(sink: _FakeSink) -> None:
    sink.subscribe("/a", buffer_size_bytes=None)
    with pytest.raises(base.TopicNotFoundError):
        sink.unsubscribe(["/a", "/zzz"])
    assert sink.subscribed_topics == ["/a"]
    sink.close()


def test_the_unsubscribe_tool_stops_topics_and_closes_an_emptied_sink(
    sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    import server

    monkeypatch.setattr(server, "_sink_class", lambda ts_type: _FakeSink)

    sink.subscribe("/a", buffer_size_bytes=None)
    sink.subscribe("/b", buffer_size_bytes=None)
    first = server.unsubscribe_live_topics("mqtt", topics=["/a"], host=sink.host, port=sink.port)
    assert first == {
        "directory": str(sink.directory),
        "unsubscribed": ["/a"],
        "still_subscribed": ["/b"],
    }
    assert sink in base.live_sinks()
    rest = server.unsubscribe_live_topics("mqtt", host=sink.host, port=sink.port)
    assert rest["unsubscribed"] == ["/b"]
    assert rest["still_subscribed"] == []
    assert sink not in base.live_sinks()  # connection released


def test_the_unsubscribe_tool_never_opens_a_new_connection(sink: _FakeSink) -> None:
    import server

    before = base.live_sinks()
    with pytest.raises(ValueError, match="No live"):
        server.unsubscribe_live_topics("mqtt", host="nowhere.invalid", port=1)
    assert base.live_sinks() == before
    sink.close()


def test_unsubscribing_an_empty_topic_list_changes_nothing(
    sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink.subscribe("/a", buffer_size_bytes=None)
    paused: list[str] = []
    monkeypatch.setattr(sink, "_unsubscribe", lambda writer: paused.append(writer.topic))
    assert sink.unsubscribe([]) == []
    assert paused == []  # an explicit [] is not "all topics"
    assert sink.subscribed_topics == ["/a"]
    sink.close()


def test_unsubscribe_keeps_a_topic_whose_fire_outlives_the_drain_window(
    sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Dropping it would let the sink close and a new overwrite subscription reset the
    # buffer files under the still-running fire.
    import time

    monkeypatch.setattr(settings, "LIVE_PIPELINE_DRAIN_SECONDS", 0.1)
    pipeline = _SlowPipeline(0.6)
    sink.subscribe("/a", pipeline=pipeline, buffer_size_bytes=None)
    writer = sink._buffers["/a"]
    writer.append({"x": 1.0})
    time.sleep(0.05)  # the fire is now running
    with pytest.raises(base.PipelineStillRunningError, match="/a"):
        sink.unsubscribe(["/a"])
    assert sink.subscribed_topics == ["/a"]
    writer.join(5)
    assert sink.unsubscribe(["/a"]) == ["/a"]  # a retry once it finished succeeds
    sink.close()


def test_the_unsubscribe_tool_matches_the_sink_type(
    sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    import server

    class _OtherSink(_FakeSink):
        pass

    monkeypatch.setattr(server, "_sink_class", lambda ts_type: _OtherSink)
    sink.subscribe("/a", buffer_size_bytes=None)
    with pytest.raises(ValueError, match="No live"):
        server.unsubscribe_live_topics("ros2.bridge", host=sink.host, port=sink.port)
    assert sink.subscribed_topics == ["/a"]
    sink.close()


def test_a_reopened_sink_waits_for_a_closed_sinks_running_fire(
    sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    # close() unregisters the sink; a new one for the same endpoint shares its buffer
    # directory, so it must not reset files a still-running fire reads.
    import time

    monkeypatch.setattr(settings, "LIVE_PIPELINE_DRAIN_SECONDS", 0.1)
    pipeline = _SlowPipeline(0.8)
    sink.subscribe("/a", pipeline=pipeline, buffer_size_bytes=None)
    old_writer = sink._buffers["/a"]
    old_writer.append({"x": 1.0})
    time.sleep(0.05)  # the fire is now running
    sink.close()
    reopened = _FakeSink(sink.host, sink.port)
    assert reopened is not sink and reopened.directory == sink.directory
    with pytest.raises(base.PipelineStillRunningError, match="/a"):
        reopened.subscribe("/a", overwrite=True, buffer_size_bytes=None)
    old_writer.join(5)
    reopened.subscribe("/a", overwrite=True, buffer_size_bytes=None)  # fine once it ended
    reopened.close()
