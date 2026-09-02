"""FleetService lifecycle: start/stop/pause/resume/status (fleet streaming spec §2/§5)."""

import dataclasses
import importlib
import json
import pathlib
import sys
import time
import types

import pyarrow as pa
import pytest

from publish.test_identity import VALID_TOKEN, fake_renew_server, fake_server
from src.sink.publish import StreamConfigError
from src.sink.publish import identity as identity_mod
from src.sink.publish import mqtt as publish_mqtt
from src.sink.publish import service as service_mod
from src.sink.publish.config import StreamsConfig
from src.sink.publish.identity import Identity
from src.sink.publish.mqtt import MqttPublisher
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.service import FleetService
from src.sink.publish.spool import Spool

# Re-exported so pytest picks them up as fixtures in this module too (they're
# only *used* below via the `fake_server`/`fake_renew_server` parameter names).
__all__ = ["fake_renew_server", "fake_server"]


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


def _fake_identity(tmp_path: pathlib.Path, expires_at: str = "2027-01-01T00:00:00Z") -> Identity:
    """A well-formed Identity pointing at files that don't need to exist for these tests.

    Nothing here does a live mTLS POST -- these tests monkeypatch
    `service_mod.renew`/`service_mod.should_attempt_renewal` directly rather
    than exercising the real transport (that's `test_identity.py`'s job), so
    the cert/key/ca paths never actually need to be read.
    """
    directory = tmp_path / "identity"
    return Identity(
        tenant="acme",
        robot_id="robot-42",
        broker_url="mqtts://fleet.example.com:8883",
        enroll_url="https://enroll.example.com",
        expires_at=expires_at,
        key_path=directory / "robot.key",
        cert_path=directory / "robot.crt",
        ca_path=directory / "ca.crt",
    )


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
                "cert_expires_at",
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
            assert status["cert_expires_at"] is None  # no identity wired in this test
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


class TestIdentityWiring:
    def test_cert_expires_at_none_without_identity(self, tmp_path: pathlib.Path) -> None:
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
            assert service.status()["cert_expires_at"] is None
            assert _wait_until(lambda: service._heartbeat.is_alive())
        finally:
            service.stop()

    def test_cert_expires_at_flows_into_status_and_heartbeat_with_identity(
        self, tmp_path: pathlib.Path
    ) -> None:
        identity = _fake_identity(tmp_path, expires_at="2027-06-01T00:00:00Z")
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=pub,
            spool=Spool(tmp_path / "spool"),
            identity=identity,
        )
        service.start()
        try:
            assert service.status()["cert_expires_at"] == "2027-06-01T00:00:00Z"
            payload = service._heartbeat_payload()
            assert payload["cert_expires_at"] == "2027-06-01T00:00:00Z"
        finally:
            service.stop()

    def test_no_identity_wires_no_renewal_check_into_heartbeat(
        self, tmp_path: pathlib.Path
    ) -> None:
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
            assert service._heartbeat._renewal_check is None
        finally:
            service.stop()

    def test_identity_wires_renewal_check_into_heartbeat(self, tmp_path: pathlib.Path) -> None:
        identity = _fake_identity(tmp_path)
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
            identity=identity,
        )
        service.start()
        try:
            # Bound methods aren't cached (`obj.meth is obj.meth` is False in
            # general), so compare the underlying function and the bound
            # instance rather than object identity of the bound method itself.
            wired = service._heartbeat._renewal_check
            assert wired is not None
            assert wired.__func__ is FleetService._renewal_check
            assert wired.__self__ is service
        finally:
            service.stop()

    def test_renewal_check_calls_renew_when_due_and_updates_identity(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Exercises `_renewal_check` directly rather than via a running
        # HeartbeatThread -- a live service.start() would tick (and thus
        # call this same closure through the real thread) immediately and
        # asynchronously, racing this test's own explicit call.
        old_identity = _fake_identity(tmp_path, expires_at="2026-09-05T00:00:00Z")  # soon
        # The real identity.renew() returns a new Identity pointing at fresh,
        # versioned file basenames (the pointer-commit scheme -- see
        # identity.py's TestRenewPointerCommitCrashSemantics), not the same
        # key_path/cert_path/ca_path as before. This fake mirrors that shape
        # rather than only changing expires_at, so this test would catch a
        # bug where the service's wiring somehow held onto stale paths
        # instead of the whole new Identity renew() hands back.
        directory = tmp_path / "identity"
        new_identity = dataclasses.replace(
            old_identity,
            expires_at="2027-09-05T00:00:00Z",
            key_path=directory / "robot-999.key",
            cert_path=directory / "robot-999.crt",
            ca_path=directory / "ca-999.crt",
        )
        renew_calls: list[Identity] = []

        monkeypatch.setattr(service_mod, "should_attempt_renewal", lambda *a, **kw: True)

        def fake_renew(identity: Identity) -> Identity:
            renew_calls.append(identity)
            return new_identity

        monkeypatch.setattr(service_mod, "renew", fake_renew)

        service = FleetService(
            sink=FakeSink({}),
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
            identity=old_identity,
        )

        service._renewal_check()

        assert renew_calls == [old_identity]
        assert service._identity is new_identity
        assert service._identity.key_path == directory / "robot-999.key"
        assert service._identity.key_path != old_identity.key_path
        assert service.status()["cert_expires_at"] == "2027-09-05T00:00:00Z"
        assert service._last_renewal_attempt_at is not None

    def test_renewal_check_skips_when_not_due(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        identity = _fake_identity(tmp_path, expires_at="2030-01-01T00:00:00Z")  # far off

        monkeypatch.setattr(service_mod, "should_attempt_renewal", lambda *a, **kw: False)

        def fail_if_called(identity: Identity) -> Identity:
            raise AssertionError("renew() must not be called when not due")

        monkeypatch.setattr(service_mod, "renew", fail_if_called)

        service = FleetService(
            sink=FakeSink({}),
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
            identity=identity,
        )

        service._renewal_check()

        assert service._identity is identity
        assert service._last_renewal_attempt_at is None

    def test_renewal_check_leaves_identity_unchanged_when_renew_returns_none(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        identity = _fake_identity(tmp_path, expires_at="2026-09-05T00:00:00Z")

        monkeypatch.setattr(service_mod, "should_attempt_renewal", lambda *a, **kw: True)
        monkeypatch.setattr(service_mod, "renew", lambda identity: None)

        service = FleetService(
            sink=FakeSink({}),
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
            identity=identity,
        )

        service._renewal_check()

        assert service._identity is identity  # unchanged: renew() reported no update
        assert service._last_renewal_attempt_at is not None  # attempt still recorded

    def test_heartbeat_tick_invokes_renewal_check_and_survives_its_exception(
        self, tmp_path: pathlib.Path
    ) -> None:
        identity = _fake_identity(tmp_path, expires_at="2030-01-01T00:00:00Z")
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        pub = FakePublisher()
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=pub,
            spool=Spool(tmp_path / "spool"),
            identity=identity,
        )
        service._renewal_check = lambda: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]
        service.start()
        try:
            # The hook raises on every tick; the heartbeat thread must stay
            # alive and keep publishing regardless.
            assert _wait_until(lambda: bool(pub.heartbeat_calls))
            assert service._heartbeat.is_alive()
        finally:
            service.stop()


class TestLastRenewalAttemptSeedsFromIdentity:
    """IMPORTANT fix: the daily rate limit must survive a process restart.

    `FleetService` used to always start with `_last_renewal_attempt_at =
    None`, regardless of what `identity.yaml` had on disk -- so a
    crashlooping robot inside the 30-day renewal window would fire a fresh
    renewal attempt roughly every restart, ignoring the daily rate limit.
    """

    def test_recent_on_disk_attempt_blocks_a_renewal_check_right_after_construction(
        self, tmp_path: pathlib.Path
    ) -> None:
        identity = _fake_identity(tmp_path, expires_at="2026-09-05T00:00:00Z")  # within window
        identity = dataclasses.replace(
            identity, last_renewal_attempt_at=time.time() - 3600
        )  # 1h ago -- well inside the 1-day rate limit

        service = FleetService(
            sink=FakeSink({}),
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
            identity=identity,
        )

        # Seeded at construction time, not left at None.
        assert service._last_renewal_attempt_at == identity.last_renewal_attempt_at

        def fail_if_called(identity: Identity) -> Identity:
            raise AssertionError(
                "renew() must not be called: rate-limited by the seeded "
                "last_renewal_attempt_at, even on a freshly constructed service"
            )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(service_mod, "renew", fail_if_called)
            service._renewal_check()  # real should_attempt_renewal: must see "not due"

    def test_no_on_disk_attempt_allows_an_immediate_renewal_check(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        identity = _fake_identity(tmp_path, expires_at="2026-09-05T00:00:00Z")
        assert identity.last_renewal_attempt_at is None

        service = FleetService(
            sink=FakeSink({}),
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
            identity=identity,
        )
        assert service._last_renewal_attempt_at is None

        renew_calls = []
        monkeypatch.setattr(service_mod, "renew", lambda ident: renew_calls.append(ident) or None)

        service._renewal_check()

        assert renew_calls == [identity]


class _FakeTlsCapturingPahoClient:
    """A minimal fake paho client that just records the kwargs `tls_set` got.

    Module-level (rather than nested in the test) purely to keep the test
    function's own cyclomatic complexity down -- this class carries no
    branching logic of its own.
    """

    def __init__(self) -> None:
        self.tls: dict[str, object] | None = None
        self._connected = False

    def will_set(self, *args: object, **kwargs: object) -> None:
        pass

    def tls_set(self, **kwargs: object) -> None:
        self.tls = kwargs

    def username_pw_set(self, *args: object, **kwargs: object) -> None:
        pass

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        self._connected = True

    def loop_start(self) -> None:
        pass

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected


def _install_fake_tls_capturing_paho(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Monkeypatch `publish_mqtt._paho` with a fake whose Client is `_FakeTlsCapturingPahoClient`.

    Returns a holder dict populated with the live client under "client" once
    `MqttPublisher.connect()` builds one -- same pattern as
    test_mqtt_publisher.py's `fake` fixture.
    """
    holder: dict[str, object] = {}

    def fake_client(**kwargs: object) -> _FakeTlsCapturingPahoClient:
        holder["client"] = _FakeTlsCapturingPahoClient()
        return holder["client"]

    fake_paho_module = types.SimpleNamespace(
        Client=fake_client,
        CallbackAPIVersion=types.SimpleNamespace(VERSION2="v2"),
        MQTTv5=5,
    )
    monkeypatch.setattr(publish_mqtt, "_paho", lambda: fake_paho_module)
    return holder


class TestRenewalReconnectPicksUpNewTlsMaterial:
    """CRITICAL fix: a live MqttPublisher must reconnect with the RENEWED cert/key.

    Before this fix, `MqttPublisher` captured `tls_certfile`/`tls_keyfile`
    paths at construction and never learned about a renewal's pointer swap
    (identity.py's `_commit_renewed_files`, which unlinks the OLD key/cert
    once the new ones are committed) -- so the first reconnect after a
    renewal tried to `tls_set()` with paths to files that no longer exist.

    This drives the real thing end to end: a real `enroll()` + real
    `renew()` against the mini-CA fake HTTP servers from test_identity.py
    (so the old files are genuinely unlinked from disk, not just
    hand-waved), a real `MqttPublisher` (paho faked out, per
    test_mqtt_publisher.py's convention), and `FleetService._renewal_check`
    -- the actual seam that must call `MqttPublisher.set_tls` before a
    caller can safely force a reconnect.
    """

    def test_renewal_check_updates_the_live_publisher_before_next_reconnect(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        directory = tmp_path / "identity"
        enrolled = identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        # _FakeRenewHandler (test_identity.py) reads the scenario back off
        # the CSR's common name -- "renew-ok" is its happy-path scenario.
        old_identity = dataclasses.replace(
            enrolled, robot_id="renew-ok", enroll_url=fake_renew_server
        )
        old_key_path, old_cert_path, old_ca_path = (
            old_identity.key_path,
            old_identity.cert_path,
            old_identity.ca_path,
        )
        assert old_key_path.exists()  # sanity: real files from a real enroll()

        fake_paho_client = _install_fake_tls_capturing_paho(monkeypatch)

        publisher = MqttPublisher(
            broker_url=old_identity.broker_url,
            tenant=old_identity.tenant,
            robot=old_identity.robot_id,
            tls_ca_certs=str(old_identity.ca_path),
            tls_certfile=str(old_identity.cert_path),
            tls_keyfile=str(old_identity.key_path),
        )
        publisher.connect()
        assert fake_paho_client["client"].tls == {
            "ca_certs": str(old_ca_path),
            "certfile": str(old_cert_path),
            "keyfile": str(old_key_path),
        }

        service = FleetService(
            sink=FakeSink({}),
            streams=_imu_streams(),
            publisher=publisher,
            spool=Spool(tmp_path / "spool"),
            identity=old_identity,
        )
        monkeypatch.setattr(service_mod, "should_attempt_renewal", lambda *a, **kw: True)

        service._renewal_check()  # runs the REAL renew() against fake_renew_server

        new_identity = service._identity
        assert new_identity is not old_identity
        assert new_identity.key_path != old_key_path
        # The pointer-swap's cleanup step really did unlink the old files.
        assert not old_key_path.exists()
        assert not old_cert_path.exists()

        publisher.connect()  # forced reconnect, e.g. the router's backoff retry

        assert fake_paho_client["client"].tls == {
            "ca_certs": str(new_identity.ca_path),
            "certfile": str(new_identity.cert_path),
            "keyfile": str(new_identity.key_path),
        }
