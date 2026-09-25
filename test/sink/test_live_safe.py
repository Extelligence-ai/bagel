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
