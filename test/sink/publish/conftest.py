"""Shared fakes for `src.sink.publish` tests: FleetService's Publisher/TopicSink doubles.

`FakeWriter`/`FakeSink`/`FakeSinkNoPrivateBuffers`/`FakePublisher` and the small
IMU-shaped `StreamsConfig`/`pa.StructType` builders started life duplicated in
`test_service.py` (and, separately, in `test_router.py` -- that copy stays
where it is: it carries router-specific behavior -- `fail_next_channel_publishes`,
`channel_publish_delay_s` -- this shared one does not need). `test_control.py`
needs the same FleetService-building doubles `test_service.py` already had, so
rather than importing test-module-to-test-module (fragile, and pytest
collection order shouldn't matter to what a test can import), both modules
import these from here.
"""

import pathlib
import threading
import time

import pyarrow as pa

from src.sink.publish.config import StreamsConfig
from src.sink.publish.identity import Identity
from src.sink.publish.publisher import Publisher, PublishError


class FakeWriter:
    """Duck-typed stand-in for TopicBufferWriter: struct + set_tap recording."""

    def __init__(self, struct: pa.StructType) -> None:
        self._struct = struct
        self._tap = None
        self.tap_calls: list[object] = []
        # Mirrors TopicBufferWriter.last_timestamp_seconds (None until a
        # message arrives) -- FleetService's health-inputs closure reads it.
        self.last_timestamp_seconds: float | None = None

    @property
    def struct(self) -> pa.StructType:
        return self._struct

    def set_tap(self, tap: object) -> None:
        self._tap = tap
        self.tap_calls.append(tap)

    def feed(self, topic: str, t: float, msg: dict) -> None:
        """Simulate a live message arriving: fire the tap, like buffer.append() does."""
        self.last_timestamp_seconds = t
        if self._tap is not None:
            self._tap(topic, t, msg)


class FakeSink:
    """Duck-typed stand-in for TopicSink: a `_buffers` dict, like the real sink."""

    def __init__(self, buffers: dict[str, FakeWriter]) -> None:
        self._buffers = buffers

    @property
    def subscribed_topics(self) -> list[str]:
        return list(self._buffers)

    def buffer_writer(self, topic: str) -> FakeWriter:
        return self._buffers[topic]


class FakeSinkNoPrivateBuffers:
    """Duck-typed TopicSink WITHOUT a `_buffers` attribute -- only the public
    `buffer_writer`/`subscribed_topics` seam. Proves `FleetService.start()` no
    longer reaches into `_buffers` directly."""

    def __init__(self, buffers: dict[str, FakeWriter]) -> None:
        self._writers_by_topic = buffers  # deliberately NOT named `_buffers`

    @property
    def subscribed_topics(self) -> list[str]:
        return list(self._writers_by_topic)

    def buffer_writer(self, topic: str) -> FakeWriter:
        return self._writers_by_topic[topic]


class FakeLiveSessionWatch:
    """In-memory double for `mqtt.LiveSessionWatch` (Codex round 3 follow-up,
    PR #214 P1, comment 3927287968's own follow-up): `.detected` is set
    directly by `FakePublisher`'s `publish()` (see `live_session_beat_after_batch`)
    to simulate a live beat arriving mid-run, without needing a real paho
    callback thread. `stop_calls` counts `.stop()` calls, mirroring the real
    `LiveSessionWatch.stop()`'s idempotency contract.
    """

    def __init__(self) -> None:
        self.detected = threading.Event()
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1


class FakePublisher(Publisher):
    """In-memory Publisher double: records every kind of publish; can fail on demand.

    `close_reasons` records the `reason` kwarg of every `close()` call, in
    order -- Task 4's pause/stop reason plumbing (spec §3) is asserted
    against this rather than a single last-reason field, so a test can also
    check a call never happened.

    `event_calls` records every `publish_event`/`publish("events", ...)`
    call (Task 7's selftest is the first caller to exercise the events lane
    end-to-end). `fail_at_channel_call`, when set, makes the Nth
    `publish_channels` call (1-indexed) raise `PublishError` instead of
    being recorded -- Task 7's selftest cleanup-on-failure test uses this to
    force a mid-run failure without a real broker.

    `calls` is a single ordered log of every method invocation (`"connect"`,
    `"live_session_probe"` from `wait_for_retained_heartbeat()`,
    `"schema"`/`"channels"`/`"heartbeat"`/`"events"` from `publish()`, and
    `"close"`) -- Task 7's selftest call-order test asserts against this
    rather than trying to interleave the separate per-kind lists.

    `live_session_beat` (Codex round 3 follow-up, PR #214 P1, comment
    3927287968) simulates what `wait_for_retained_heartbeat()` would
    return on a real `MqttPublisher`: `None` (default -- no retained
    heartbeat, matching a fresh robot/broker), or a payload dict such as
    `{"online": True}` (a live session connected) / `{"online": False}`
    (paused/stopped/lwt). `disconnect_without_publishing_calls` counts
    calls to the matching silent-teardown helper `_check_no_live_session`
    uses on refusal (comment 3927287968) -- distinct from `close_calls`,
    since that path must NEVER publish a clean-stop beat.

    `live_session_beat_after_batch` (Codex round 3 follow-up, PR #214 P1,
    comment 3927287968's own follow-up) simulates a live service RESUMING
    mid-run: when set, the Nth (1-indexed) successful `publish_channels`
    call sets the open `FakeLiveSessionWatch`'s `.detected` Event, exactly
    as a real `LiveSessionWatch`'s paho callback would upon receiving a
    later `online: true` beat. `watch_live_session()` -- the ongoing-watch
    capability `run_selftest` polls between batches and before the
    heartbeat/event publishes -- returns a `FakeLiveSessionWatch`
    (`watch_open_calls` counts calls to it).
    """

    def __init__(
        self,
        *,
        connect_should_fail: bool = False,
        fail_at_channel_call: int | None = None,
        live_session_beat: dict | None = None,
        live_session_beat_after_batch: int | None = None,
    ) -> None:
        self.connect_should_fail = connect_should_fail
        self.fail_at_channel_call = fail_at_channel_call
        self.live_session_beat = live_session_beat
        self.live_session_beat_after_batch = live_session_beat_after_batch
        self.connect_calls = 0
        self.schema_calls: list[dict] = []
        self.channel_calls: list[dict] = []
        self.heartbeat_calls: list[dict] = []
        self.event_calls: list[dict] = []
        self.close_calls = 0
        self.close_reasons: list[str] = []
        self.reconnects = 0
        self.calls: list[str] = []
        self.live_session_probe_calls = 0
        self.disconnect_without_publishing_calls = 0
        self.watch_open_calls = 0
        self._watch: FakeLiveSessionWatch | None = None
        self._connected = False

    def connect(self) -> None:
        self.calls.append("connect")
        self.connect_calls += 1
        if self.connect_should_fail:
            raise PublishError("connect failed")
        self._connected = True

    def wait_for_retained_heartbeat(self, timeout_s: float = 1.5) -> dict | None:
        self.calls.append("live_session_probe")
        self.live_session_probe_calls += 1
        return self.live_session_beat

    def watch_live_session(self) -> FakeLiveSessionWatch:
        self.calls.append("watch_open")
        self.watch_open_calls += 1
        self._watch = FakeLiveSessionWatch()
        return self._watch

    def disconnect_without_publishing(self) -> None:
        self.calls.append("disconnect_without_publishing")
        self.disconnect_without_publishing_calls += 1
        self._connected = False

    def publish(
        self, kind: str, payload: dict, *, retain: bool = False, timeout_s: float = 10.0
    ) -> None:
        self.calls.append(kind)
        if kind == "schema":
            self.schema_calls.append(payload)
        elif kind == "channels":
            call_index = len(self.channel_calls) + 1
            if self.fail_at_channel_call == call_index:
                raise PublishError(f"forced failure at channel batch {call_index}")
            self.channel_calls.append(payload)
            if (
                self.live_session_beat_after_batch is not None
                and call_index == self.live_session_beat_after_batch
                and self._watch is not None
            ):
                self._watch.detected.set()
        elif kind == "heartbeat":
            self.heartbeat_calls.append(payload)
        elif kind == "events":
            self.event_calls.append(payload)
        else:
            raise AssertionError(f"FakePublisher: unexpected kind {kind!r}")

    def close(self, reason: str = "stopped") -> None:
        self.calls.append("close")
        self.close_calls += 1
        self.close_reasons.append(reason)
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
