"""Sink surface for fleet taps (spec §2): public buffer accessor, live-sink listing,
and close hooks.

Uses a minimal `base.TopicSink` subclass (mirrors test_admission.py's `_FakeSink`)
rather than the MQTT `make_sink` fixture, since none of this needs a live broker.
"""

import itertools
import logging
import pathlib

import pyarrow as pa
import pytest

from settings import settings
from src.sink import base
from src.sink.base import TopicNotFoundError
from src.sink.buffer import TopicBufferWriter

_port_counter = itertools.count(21000)


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
    return _FakeSink("localhost", next(_port_counter))


@pytest.fixture(autouse=True)
def _clear_close_hooks() -> None:
    """Isolate each test's hook registrations from the others (and from startup.py's
    own module-scope registration, which is not imported by this test module)."""
    saved = list(base._close_hooks)
    base._close_hooks.clear()
    yield
    base._close_hooks[:] = saved


class TestBufferWriter:
    def test_returns_the_same_object_as_buffers(self, sink: _FakeSink) -> None:
        sink.subscribe("/a")
        assert sink.buffer_writer("/a") is sink._buffers["/a"]

    def test_unknown_topic_raises_topic_not_found_error(self, sink: _FakeSink) -> None:
        with pytest.raises(TopicNotFoundError):
            sink.buffer_writer("/nope")


class TestLiveSinks:
    def test_returns_all_live_singletons(
        self, sink: _FakeSink, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(sink.directory.parent))
        other = _FakeSink("localhost", next(_port_counter))
        try:
            live = base.live_sinks()
            assert sink in live
            assert other in live
        finally:
            other.close()

    def test_is_a_snapshot_copy(self, sink: _FakeSink) -> None:
        live = base.live_sinks()
        live.append("not-a-real-sink")
        assert "not-a-real-sink" not in base.live_sinks()


class TestCloseHooks:
    def test_fires_on_close_with_the_sink_instance(self, sink: _FakeSink) -> None:
        seen = []
        base.register_close_hook(seen.append)
        sink.close()
        assert seen == [sink]

    def test_raising_hook_is_swallowed_and_close_still_completes(
        self, sink: _FakeSink, caplog: pytest.LogCaptureFixture
    ) -> None:
        def boom(_sink: object) -> None:
            raise RuntimeError("hook exploded")

        base.register_close_hook(boom)
        with caplog.at_level(logging.WARNING):
            sink.close()  # must not raise
        assert any("hook exploded" in record.getMessage() for record in caplog.records) or any(
            record.exc_info for record in caplog.records
        )

    def test_unregister_stops_delivery(self, sink: _FakeSink) -> None:
        seen = []
        unregister = base.register_close_hook(seen.append)
        unregister()
        sink.close()
        assert seen == []

    def test_unregister_is_idempotent(self, sink: _FakeSink) -> None:
        unregister = base.register_close_hook(lambda _s: None)
        unregister()
        unregister()  # must not raise

    def test_hooks_fire_before_buffers_are_popped(self, sink: _FakeSink) -> None:
        sink.subscribe("/a")
        seen_subscribed_topics = []

        def hook(s: object) -> None:
            seen_subscribed_topics.append(list(s.subscribed_topics))

        base.register_close_hook(hook)
        sink.close()

        assert seen_subscribed_topics == [["/a"]]
