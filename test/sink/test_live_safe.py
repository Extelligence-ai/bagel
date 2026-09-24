"""Attaching a batch-only pipeline to a live subscription is refused (Codex P1)."""

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
        return ["/a"]

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
    return _FakeSink("localhost", next(_port_counter))


def _anomaly_pipeline(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> pipeline_base.Pipeline:
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    log = write_fault_log(tmp_path / "log.mcap", duration_seconds=30)
    return pipeline_base.Pipeline.build(
        {
            "name": "p",
            "site": "s",
            "asset": "a",
            "path": str(log),
            "allow_failure": False,
            "cadence": {"topic": "/motor/current", "when": {"every": 10, "unit": "second"}},
            "gates": [
                {
                    "module": "src.pipeline.gates.anomaly",
                    "lookback": {"last": 10, "unit": "second"},
                    "args": {"anomalies": {"overcurrent": "too much current"}},
                }
            ],
            "tasks": [{"module": "src.pipeline.tasks.write_annotations"}],
        }
    )


def test_subscribing_a_fresh_topic_with_a_batch_only_pipeline_is_refused(
    sink: _FakeSink, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The normal subscribe_live_topics path: the topic is not yet subscribed.
    with pytest.raises(ValueError, match="batch-only"):
        sink.subscribe("/a", pipeline=_anomaly_pipeline(tmp_path, monkeypatch))
    assert "/a" not in sink._buffers


def test_subscribing_without_a_pipeline_still_works(sink: _FakeSink) -> None:
    sink.subscribe("/a", buffer_size_bytes=None)
    assert "/a" in sink._buffers


def test_multi_topic_subscription_is_refused_before_any_topic_is_subscribed(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # subscribe_live_topics subscribes topics in order; with the cadence topic listed
    # last, earlier topics used to be subscribed before the guard fired, leaving a
    # partial live subscription with no way to undo it (Codex P2).
    from src.sink import startup

    class _TwoTopicSink(_FakeSink):
        def _available_topics(self) -> list[str]:
            return ["/a", "/b"]

    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    sink = _TwoTopicSink("localhost", next(_port_counter))
    config = {
        "name": "p",
        "site": "s",
        "asset": "a",
        "allow_failure": False,
        "cadence": {"topic": "/b", "when": {"every": 10, "unit": "second"}},
        "gates": [
            {
                "module": "src.pipeline.gates.anomaly",
                "lookback": {"last": 10, "unit": "second"},
                "args": {"anomalies": {"overcurrent": "too much current"}},
            }
        ],
        "tasks": [{"module": "src.pipeline.tasks.write_annotations"}],
    }
    with pytest.raises(ValueError, match="batch-only"):
        startup.subscribe_with_pipeline(sink, ["/a", "/b"], config)
    assert sink._buffers == {}
