"""Admission control for live-buffer disk usage (#134).

Before hardening, N subscribed topics could claim N x buffer_size_bytes (x2
transiently) with no global bound. SINK_TOTAL_BUFFER_BYTES=0 preserves that
behavior; when set, subscribe() refuses topics that would exceed the total.
"""

import itertools
import pathlib

import pyarrow as pa
import pytest

from settings import settings
from src.sink import base
from src.sink.buffer import TopicBufferWriter

_port_counter = itertools.count(19000)


class _FakeSink(base.TopicSink):
    def _connect(self) -> None:
        pass

    def _disconnect(self) -> None:
        pass

    def _available_topics(self) -> list[str]:
        return ["/a", "/b", "/c"]

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
    # unique port per test: TopicSink is a (host, port)-keyed singleton
    return _FakeSink("localhost", next(_port_counter))


def test_default_zero_admits_unbounded(sink: _FakeSink, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "SINK_TOTAL_BUFFER_BYTES", 0)
    sink.subscribe("/a", buffer_size_bytes=None)
    sink.subscribe("/b", buffer_size_bytes=10**12)


def test_cap_admits_until_exceeded(sink: _FakeSink, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "SINK_TOTAL_BUFFER_BYTES", 2_000)
    sink.subscribe("/a", buffer_size_bytes=1_000)
    sink.subscribe("/b", buffer_size_bytes=1_000)
    with pytest.raises(base.BufferCapacityExceededError) as excinfo:
        sink.subscribe("/c", buffer_size_bytes=1_000)
    message = str(excinfo.value)
    assert "SINK_TOTAL_BUFFER_BYTES" in message
    assert "buffer_size_bytes" in message


def test_cap_rejects_unbounded_topic(sink: _FakeSink, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "SINK_TOTAL_BUFFER_BYTES", 2_000)
    with pytest.raises(base.BufferCapacityExceededError):
        sink.subscribe("/a", buffer_size_bytes=None)


def test_overwrite_replaces_rather_than_adds(
    sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "SINK_TOTAL_BUFFER_BYTES", 2_000)
    sink.subscribe("/a", buffer_size_bytes=1_500)
    sink.subscribe("/a", buffer_size_bytes=1_800, overwrite=True)  # replaces, still fits
