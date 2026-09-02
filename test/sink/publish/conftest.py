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


class FakePublisher(Publisher):
    """In-memory Publisher double: records every kind of publish; can fail on demand.

    `close_reasons` records the `reason` kwarg of every `close()` call, in
    order -- Task 4's pause/stop reason plumbing (spec §3) is asserted
    against this rather than a single last-reason field, so a test can also
    check a call never happened.
    """

    def __init__(self, *, connect_should_fail: bool = False) -> None:
        self.connect_should_fail = connect_should_fail
        self.connect_calls = 0
        self.schema_calls: list[dict] = []
        self.channel_calls: list[dict] = []
        self.heartbeat_calls: list[dict] = []
        self.close_calls = 0
        self.close_reasons: list[str] = []
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

    def close(self, reason: str = "stopped") -> None:
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
