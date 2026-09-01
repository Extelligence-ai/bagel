"""FleetService lifecycle: start/stop/pause/resume/status (fleet streaming spec §2/§5)."""

import importlib
import json
import pathlib
import sys
import time

import pyarrow as pa
import pytest

from src.sink.publish import StreamConfigError
from src.sink.publish.config import StreamsConfig
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.service import FleetService
from src.sink.publish.spool import Spool


def test_service_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.service", raising=False)
    importlib.import_module("src.sink.publish.service")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)


class FakeWriter:
    """Duck-typed stand-in for TopicBufferWriter: struct + set_tap recording."""

    def __init__(self, struct: pa.StructType) -> None:
        self._struct = struct
        self._tap = None
        self.tap_calls: list[object] = []

    @property
    def struct(self) -> pa.StructType:
        return self._struct

    def set_tap(self, tap: object) -> None:
        self._tap = tap
        self.tap_calls.append(tap)

    def feed(self, topic: str, t: float, msg: dict) -> None:
        """Simulate a live message arriving: fire the tap, like buffer.append() does."""
        if self._tap is not None:
            self._tap(topic, t, msg)


class FakeSink:
    """Duck-typed stand-in for TopicSink: a `_buffers` dict, like the real sink."""

    def __init__(self, buffers: dict[str, FakeWriter]) -> None:
        self._buffers = buffers

    @property
    def subscribed_topics(self) -> list[str]:
        return list(self._buffers)


class FakePublisher(Publisher):
    """In-memory Publisher double: records every kind of publish; can fail on demand."""

    def __init__(self, *, connect_should_fail: bool = False) -> None:
        self.connect_should_fail = connect_should_fail
        self.connect_calls = 0
        self.schema_calls: list[dict] = []
        self.channel_calls: list[dict] = []
        self.heartbeat_calls: list[dict] = []
        self.close_calls = 0
        self.reconnects = 0
        self._connected = False

    def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_should_fail:
            raise PublishError("connect failed")
        self._connected = True

    def publish(
        self, kind: str, payload: dict, *, retain: bool = False, timeout_s: float = 10.0
    ) -> None:
        if kind == "schema":
            self.schema_calls.append(payload)
        elif kind == "channels":
            self.channel_calls.append(payload)
        elif kind == "heartbeat":
            self.heartbeat_calls.append(payload)
        else:
            raise AssertionError(f"FakePublisher: unexpected kind {kind!r}")

    def close(self) -> None:
        self.close_calls += 1
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected


def _imu_streams(**overrides: object) -> StreamsConfig:
    config = {
        "flush_interval_s": overrides.pop("flush_interval_s", 0.05),
        "channels": [{"topic": "/imu", "fields": ["x"], "rate_hz": 50.0}],
    }
    config.update(overrides)
    return StreamsConfig.build(config)


def _imu_struct() -> pa.StructType:
    return pa.struct([pa.field("x", pa.float64())])


def _wait_until(predicate: object, timeout_s: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class TestStart:
    def test_wires_taps_and_publishes_schema_on_first_pump(self, tmp_path: pathlib.Path) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        spool = Spool(tmp_path / "spool")
        service = FleetService(sink=sink, streams=_imu_streams(), publisher=pub, spool=spool)

        service.start()
        try:
            assert writer._tap is not None
            assert _wait_until(lambda: bool(pub.schema_calls))
            assert pub.schema_calls[0] == {
                "v": 1,
                "channels": [
                    {
                        "c": "imu.x",
                        "type": "number",
                        "unit": None,
                        "source_topic": "/imu",
                        "source_field": "x",
                    }
                ],
            }
        finally:
            service.stop()

    def test_unsubscribed_topic_raises_stream_config_error(self, tmp_path: pathlib.Path) -> None:
        sink = FakeSink({})  # /imu never subscribed
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
        )
        with pytest.raises(StreamConfigError):
            service.start()

    def test_live_sample_flows_to_channels(self, tmp_path: pathlib.Path) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        spool = Spool(tmp_path / "spool")
        service = FleetService(sink=sink, streams=_imu_streams(), publisher=pub, spool=spool)

        service.start()
        try:
            assert _wait_until(lambda: bool(pub.schema_calls))
            writer.feed("/imu", 1.0, {"x": 3.5})
            assert _wait_until(lambda: bool(pub.channel_calls))
            samples = pub.channel_calls[0]["samples"]
            assert samples == [{"c": "imu.x", "t": 1.0, "v": 3.5}]
        finally:
            service.stop()

    def test_double_start_raises_runtime_error(self, tmp_path: pathlib.Path) -> None:
        # A double start is a programming error -- raise, don't silently
        # no-op or silently restart the runtime out from under the caller.
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
        )
        service.start()
        try:
            with pytest.raises(RuntimeError, match="already started"):
                service.start()
        finally:
            service.stop()


class TestStop:
    def test_idempotent_and_clears_taps(self, tmp_path: pathlib.Path) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        service = FleetService(
            sink=sink, streams=_imu_streams(), publisher=pub, spool=Spool(tmp_path / "spool")
        )
        service.start()
        assert writer._tap is not None

        service.stop()
        assert writer._tap is None
        assert pub.close_calls == 1

        service.stop()  # idempotent: no double-close, no error
        assert pub.close_calls == 1

    def test_heartbeat_stops_before_publisher_close(self, tmp_path: pathlib.Path) -> None:
        # The stopped-heartbeat publish happens inside publisher.close(); if the
        # heartbeat thread were still alive it could race a live tick in after
        # that one. Assert ordering by observing no heartbeat ticks land on the
        # publisher AFTER close_calls goes to 1 beyond the close-time state.
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=pub,
            spool=Spool(tmp_path / "spool"),
        )
        service.start()
        service.stop()
        assert not service._heartbeat.is_alive()
        # No heartbeat tick may be published by our own thread after stop()
        # returns -- publisher.close() (a FakePublisher no-op here beyond the
        # counter) is the last word, and nothing else calls publish_heartbeat.
        calls_at_stop = len(pub.heartbeat_calls)
        time.sleep(0.05)
        assert len(pub.heartbeat_calls) == calls_at_stop


class TestPauseResume:
    def test_pause_discard_empties_channels_lane(self, tmp_path: pathlib.Path) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        spool = Spool(tmp_path / "spool")
        service = FleetService(sink=sink, streams=_imu_streams(), publisher=pub, spool=spool)
        service.start()
        try:
            seq = spool.next_seq("channels")
            spool.append("channels", seq, {"v": 1, "samples": []})
            assert list(spool.pending("channels"))

            service.pause(discard=True)
            assert list(spool.pending("channels")) == []
            assert writer._tap is None
        finally:
            service.stop()

    def test_pause_without_discard_keeps_channels_lane(self, tmp_path: pathlib.Path) -> None:
        # connect_should_fail keeps the router offline for the test's whole
        # life, so the manually-appended record below is guaranteed to still
        # be pending at pause() -- with a live publisher this races the
        # router thread's own _pump, which would drain and ack it first.
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher(connect_should_fail=True)
        spool = Spool(tmp_path / "spool")
        service = FleetService(sink=sink, streams=_imu_streams(), publisher=pub, spool=spool)
        service.start()
        try:
            seq = spool.next_seq("channels")
            spool.append("channels", seq, {"v": 1, "samples": []})

            service.pause(discard=False)
            assert list(spool.pending("channels"))
        finally:
            service.stop()

    def test_pause_and_resume_are_idempotent_and_restart_threads(
        self, tmp_path: pathlib.Path
    ) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        service = FleetService(
            sink=sink, streams=_imu_streams(), publisher=pub, spool=Spool(tmp_path / "spool")
        )
        service.start()
        try:
            first_router = service._router
            service.pause()
            assert not first_router.is_alive()
            assert writer._tap is None
            closes_after_first_pause = pub.close_calls

            service.pause()  # idempotent: no second close, no error
            assert pub.close_calls == closes_after_first_pause

            service.resume()
            assert writer._tap is not None
            assert service._router is not first_router
            assert service._router.is_alive()

            service.resume()  # idempotent: no error, no second restart
            assert service._router.is_alive()
        finally:
            service.stop()


class TestStatus:
    def test_shape_and_json_serializable(self, tmp_path: pathlib.Path) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        service = FleetService(
            sink=sink, streams=_imu_streams(), publisher=pub, spool=Spool(tmp_path / "spool")
        )
        service.start()
        try:
            status = service.status()
            json.dumps(status)  # must not raise: plain JSON-able
            assert set(status) == {
                "online",
                "backoff",
                "queue",
                "skipped",
                "spool",
                "reconnects",
                "subscriptions",
                "channels_active",
                "router_alive",
                "router_error",
                "heartbeat_spool_failures",
                "heartbeat_alive",
                "heartbeat_error",
            }
            assert status["subscriptions"] == ["/imu"]
            assert status["channels_active"] == 1
            assert set(status["queue"]) == {"depth", "dropped"}
            assert isinstance(status["spool"], dict)
            assert status["router_alive"] is True
            assert status["router_error"] is None
            assert status["heartbeat_spool_failures"] == 0
            assert status["heartbeat_alive"] is True
            assert status["heartbeat_error"] is None
        finally:
            service.stop()

    def test_before_start_is_safe_and_json_serializable(self, tmp_path: pathlib.Path) -> None:
        sink = FakeSink({})
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
        )
        status = service.status()
        json.dumps(status)
        assert status["online"] is False
        assert status["router_alive"] is False
        assert status["heartbeat_alive"] is False
        assert status["heartbeat_error"] is None

    def test_dead_router_is_visible(self, tmp_path: pathlib.Path) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        service = FleetService(
            sink=sink, streams=_imu_streams(), publisher=pub, spool=Spool(tmp_path / "spool")
        )
        service.start()
        try:
            assert _wait_until(lambda: bool(pub.schema_calls))

            def boom(*_a: object, **_kw: object) -> None:
                raise RuntimeError("core exploded")

            service._router._core.offer = boom  # type: ignore[method-assign]
            writer.feed("/imu", 1.0, {"x": 1.0})

            assert _wait_until(lambda: not service._router.is_alive())
            status = service.status()
            json.dumps(status)
            assert status["router_alive"] is False
            assert status["router_error"] is not None and "core exploded" in status["router_error"]
        finally:
            service.stop()
