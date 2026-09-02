"""Live-broker end-to-end tests for fleet streaming (spec §2/§3/§4).

Gated on MQTT_TEST_BROKER (e.g. mqtt://localhost:1883), same as step 3's
test_mqtt_integration.py. Unlike that file, this one drives the FULL
runtime -- a real TopicBufferWriter feeding a real FleetService (real
MqttPublisher, real tmp-path Spool) -- to prove the tap -> queue ->
RouterCore -> StreamRouter -> spool -> MqttPublisher pipeline works against
an actual broker, including the chaos path: kill the broker mid-stream,
keep appending, restart it, and confirm the spool replays its backlog in
order with nothing lost.

Locally:
    docker run -d -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
    MQTT_TEST_BROKER=mqtt://localhost:1883 uv run pytest test/sink/publish/test_stream_e2e.py

The chaos test additionally requires MQTT_TEST_BROKER_MANAGED=1: it starts,
kills, and restarts its OWN mosquitto container on MQTT_CHAOS_PORT (default
1884) -- a different container on a different port from whatever
MQTT_TEST_BROKER points at, so it can never kill a broker this test didn't
start itself (in CI, the iot job's shared `mosq` container on 1883, which
the rest of that job's suite still needs).
"""

import functools
import json
import os
import pathlib
import queue
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from src.sink.publish.config import StreamsConfig
from src.sink.publish.publisher import wire_topic
from src.sink.publish.spool import Spool

if TYPE_CHECKING:
    from src.sink.buffer import TopicBufferWriter

BROKER = os.environ.get("MQTT_TEST_BROKER")
MANAGED = os.environ.get("MQTT_TEST_BROKER_MANAGED") == "1"
CHAOS_PORT = int(os.environ.get("MQTT_CHAOS_PORT", "1884"))

pytestmark = pytest.mark.skipif(not BROKER, reason="MQTT_TEST_BROKER not set")

requires_managed_broker = pytest.mark.skipif(
    not MANAGED,
    reason=(
        "MQTT_TEST_BROKER_MANAGED=1 required: this test kills and restarts its own broker container"
    ),
)

IMU_STRUCT = pa.struct([pa.field("x", pa.float64())])

Inbox = "queue.Queue[tuple[str, bytes, bool]]"


# -- generic polling (no bare sleeps for correctness: deadline + short sleep) -----------


def _wait_until(predicate: Callable[[], bool], timeout_s: float, interval_s: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return predicate()


def _tcp_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _wait_tcp(host: str, port: int, timeout_s: float) -> bool:
    return _wait_until(lambda: _tcp_reachable(host, port), timeout_s=timeout_s, interval_s=0.05)


def _drain(inbox: Inbox, topic: str, timeout_s: float = 10.0) -> tuple[str, bytes, bool] | None:
    """Pull from `inbox` until `topic` matches or the deadline passes (step 3's pattern)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            got = inbox.get(timeout=deadline - time.time())
        except queue.Empty:
            return None
        if got[0] == topic:
            return got
    return None


# -- fixtures shared by the three scenarios ---------------------------------------------


class FakeSink:
    """Minimal TopicSink double: the `_buffers`/`subscribed_topics` seam FleetService uses."""

    def __init__(self, buffers: dict[str, "TopicBufferWriter"]) -> None:
        self._buffers = buffers

    @property
    def subscribed_topics(self) -> list[str]:
        return list(self._buffers)


def _real_writer(tmp_path: pathlib.Path, topic: str = "/imu") -> "TopicBufferWriter":
    """A real TopicBufferWriter (not a fake) backed by a tmp cache -- brief's step 1."""
    from src.sink.buffer import TopicBufferWriter

    return TopicBufferWriter(
        path=tmp_path / "buffer",
        topic=topic,
        type_name="test/Imu",
        definition="float64 x",
        struct=IMU_STRUCT,
        buffer_size_bytes=None,
        overwrite=False,
        pipeline=None,
        extract_timestamp=lambda msg: msg["timestamp_seconds"],
    )


def _streams(flush_interval_s: float, rate_hz: float, topic: str = "/imu") -> StreamsConfig:
    return StreamsConfig.build(
        {
            "flush_interval_s": flush_interval_s,
            "channels": [{"topic": topic, "fields": ["x"], "rate_hz": rate_hz}],
        }
    )


class _Subscriber:
    """One paho client subscribed to a robot's whole `bagel/v1/<tenant>/<robot>/#` tree.

    `on_connect` always resubscribes -- covers both the initial connect and any
    background auto-reconnect paho performs on its own after a drop. The
    chaos test additionally drives recovery explicitly via `reconnect_now`
    instead of waiting on paho's own backoff-gated auto-reconnect: paho's
    default reconnect delay (min 1s, doubling) is the same order of
    magnitude as StreamRouter's own backoff, so racing them is not reliable
    -- a QoS-1 `channels` publish that lands before this subscriber has
    resubscribed is lost for good (no persistent session survives the
    container restart). `reconnect_now` bypasses that race by polling raw
    TCP reachability directly and reconnecting the instant it succeeds.
    """

    def __init__(self, host: str, port: int, tenant: str, robot: str) -> None:
        import paho.mqtt.client as paho

        self.inbox: Inbox = queue.Queue()
        self._host = host
        self._port = port
        self._topic = f"bagel/v1/{tenant}/{robot}/#"
        self._subscribed = threading.Event()
        self._client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=f"sub-{uuid.uuid4().hex[:8]}",
            protocol=paho.MQTTv5,
        )
        self._client.on_connect = self._on_connect
        self._client.on_subscribe = self._on_subscribe
        self._client.on_message = lambda cl, ud, msg: self.inbox.put(
            (msg.topic, msg.payload, msg.retain)
        )
        # paho's own auto-reconnect waits >= min_delay (default 1s, doubling)
        # before retrying a dropped connection -- an eternity against
        # StreamRouter's jittered post-restart reconnect (see
        # `reconnect_now`'s docstring). Keep any paho-internal retry this
        # client ever performs fast too (floats are fine at runtime; the
        # int annotation is just paho's signature).
        self._client.reconnect_delay_set(min_delay=0.1, max_delay=0.5)

    def _on_connect(
        self, client: object, userdata: object, flags: object, reason_code: object, *_a: object
    ) -> None:
        self._subscribed.clear()
        client.subscribe(self._topic, qos=1)  # `client` is paho's Client (duck-typed as object)

    def _on_subscribe(self, *_a: object) -> None:
        self._subscribed.set()

    def connect(self, timeout_s: float = 10.0) -> None:
        self._client.connect(self._host, self._port, 30)
        self._client.loop_start()
        assert _wait_until(self._subscribed.is_set, timeout_s=timeout_s), (
            "subscriber never subscribed"
        )

    def prepare_for_outage(self) -> None:
        """Stop the background loop right after the broker is killed.

        Split out of `reconnect_now` so `loop_stop`'s thread-join cost (it
        can take a moment to notice and exit) is paid during the outage,
        when there is no time pressure -- not stacked onto the recovery
        path where it would only widen the race `reconnect_now` exists to
        avoid.
        """
        self._client.loop_stop()
        self._subscribed.clear()

    def reconnect_now(self, timeout_s: float = 15.0) -> None:
        """Hammer a raw reconnect until this client is actually RESUBSCRIBED.

        Call `prepare_for_outage()` first (right after killing the broker).
        Polls tightly (10ms) rather than waiting on a separate "is the port
        up yet" check first: any extra step between the broker actually
        accepting connections and this client's own reconnect attempt only
        widens the race against StreamRouter's own reconnect (see class
        docstring).

        Critically, a TCP-level `reconnect()` success is verified all the
        way to SUBACK before it is trusted: a freshly-restarted container
        can accept the TCP connection before mosquitto is actually serving
        sessions, and a connection that dies between that accept and the
        CONNACK/SUBACK would otherwise be retried only by paho's
        auto-reconnect after its built-in delay. That delay is long enough
        for StreamRouter's jittered reconnect to land and drain the entire
        QoS-1 backlog while nobody is subscribed -- no persistent session
        survives the container restart, so every batch published before
        this client's SUBACK is gone for good (observed live as the chaos
        test's "gap or missing seq: delivered [1, 38, 39]" CI failure: an
        instrumented run showed the router reconnecting 0.95s after the
        broker came back and finishing the whole drain before this
        client's SUBACK at +1.85s). A session that hasn't reached SUBACK
        within its attempt budget is torn down and re-attempted
        immediately instead.
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                self._client.reconnect()
            except OSError:
                time.sleep(0.01)
                continue
            self._client.loop_start()
            if _wait_until(self._subscribed.is_set, timeout_s=2.0, interval_s=0.005):
                return
            # Stillborn session (TCP accepted, SUBACK never arrived): drop
            # it and hammer again ourselves rather than waiting out paho.
            self._client.loop_stop()
        raise AssertionError("subscriber never resubscribed after the broker restart")

    def close(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()


def _docker(args: list[str], *, check: bool) -> subprocess.CompletedProcess:
    """Run one `docker` CLI invocation. Fixed argv, no untrusted input."""
    return subprocess.run(args, check=check, capture_output=True)  # noqa: S603


class _ChaosBroker:
    """A private mosquitto container this test starts, kills, and restarts.

    Runs on `MQTT_CHAOS_PORT` (default 1884) -- deliberately never the CI iot
    job's shared `mosq` container on 1883, so killing it can never disrupt
    the rest of that job's suite (see module docstring).
    """

    def __init__(self, port: int) -> None:
        self.port = port
        self.name = f"bagel-mosq-chaos-{uuid.uuid4().hex[:8]}"

    def start(self) -> None:
        """Start the container; self-cleaning if it never becomes reachable.

        `docker run -d` succeeding only means the container process was
        created, not that mosquitto is actually listening yet -- if the
        readiness wait below fails (slow image pull, port contention), the
        container would otherwise leak: `docker run` already succeeded, so
        nothing else would ever `docker rm` it. Catch, clean up the
        container THIS call just created, and re-raise so the failure is
        still visible to the caller.
        """
        _docker(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.name,
                "-p",
                f"{self.port}:1883",
                "eclipse-mosquitto:2",
                "mosquitto",
                "-c",
                "/mosquitto-no-auth.conf",
            ],
            check=True,
        )
        try:
            assert _wait_tcp("localhost", self.port, timeout_s=15.0), (
                "chaos broker never became reachable"
            )
        except BaseException:
            self.cleanup()
            raise

    def kill(self) -> None:
        _docker(["docker", "kill", self.name], check=True)

    def restart(self) -> None:
        _docker(["docker", "start", self.name], check=True)
        assert _wait_tcp("localhost", self.port, timeout_s=15.0), "chaos broker never came back up"

    def cleanup(self) -> None:
        _docker(["docker", "rm", "-f", self.name], check=False)


@requires_managed_broker
def test_chaos_broker_start_cleans_up_on_unreachable_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """start() must not leak a container when readiness never arrives.

    Real `docker run`/`docker ps`/`docker rm` -- that's the actual leak this
    guards against -- with only `_wait_tcp` faked, so the "broker never
    becomes reachable" path (slow image pull, port contention -- the
    scenario in the review finding) is forced deterministically and fast
    without needing a real slow pull or a real port conflict.
    """
    monkeypatch.setattr(sys.modules[__name__], "_wait_tcp", lambda *a, **kw: False)
    broker = _ChaosBroker(CHAOS_PORT)
    try:
        with pytest.raises(AssertionError, match="never became reachable"):
            broker.start()
        result = _docker(
            ["docker", "ps", "-a", "--filter", f"name={broker.name}", "--format", "{{.Names}}"],
            check=True,
        )
        assert result.stdout.decode().strip() == "", (
            f"container {broker.name} leaked after a failed start()"
        )
    finally:
        broker.cleanup()  # belt-and-braces: start() should already have removed it


class _StillbornThenLiveClient:
    """Fake paho client: the first reconnect() is a TCP-level success whose
    session dies before CONNACK/SUBACK ever arrives (a freshly-restarted
    container accepting the connection before mosquitto actually serves it);
    any later reconnect() lands on a live broker and the CONNACK ->
    SUBSCRIBE -> SUBACK flow completes as soon as the loop runs.
    """

    def __init__(self, subscribed: threading.Event) -> None:
        self._subscribed = subscribed
        self.reconnect_calls = 0
        self.loop_stops = 0

    def reconnect(self) -> int:
        self.reconnect_calls += 1
        return 0

    def loop_start(self) -> None:
        if self.reconnect_calls >= 2:
            self._subscribed.set()

    def loop_stop(self) -> None:
        self.loop_stops += 1


def test_reconnect_now_retries_a_stillborn_tcp_connection() -> None:
    """A TCP-level reconnect() success is not a recovered subscriber.

    Pins the chaos test's own recovery helper against the failure mode
    observed live (instrumented run: sub SUBACK 1.85s after the broker came
    back, router reconnect + full backlog drain at +0.95s, delivered seqs
    [1] / CI's [1, 38, 39]): when `reconnect_now`'s first successful
    `reconnect()` latches a connection that dies before SUBACK, it must tear
    that session down and hammer again itself -- never sit out paho's >=1s
    auto-reconnect delay, which is long enough for StreamRouter's jittered
    reconnect to drain the whole QoS-1 backlog with nobody subscribed. No
    broker involved: a fake client simulates the stillborn-then-live broker.
    """
    sub = _Subscriber.__new__(_Subscriber)  # no real paho client, no broker
    sub._subscribed = threading.Event()
    client = _StillbornThenLiveClient(sub._subscribed)
    sub._client = client
    started = time.monotonic()
    sub.reconnect_now(timeout_s=8.0)
    elapsed = time.monotonic() - started
    assert sub._subscribed.is_set()
    assert client.reconnect_calls >= 2, (
        "reconnect_now trusted a single TCP-level reconnect() that never "
        "reached SUBACK instead of retrying it"
    )
    assert client.loop_stops >= 1, "the stillborn session was never torn down"
    assert elapsed < 6.0, "recovery took long enough for the router to drain the backlog"


def _build_service(  # noqa: PLR0913 -- test helper collecting one scenario's full config
    tmp_path: pathlib.Path,
    broker_url: str,
    tenant: str,
    robot: str,
    *,
    flush_interval_s: float,
    rate_hz: float,
) -> tuple["TopicBufferWriter", Spool, object]:
    from src.sink.publish.mqtt import MqttPublisher
    from src.sink.publish.service import FleetService

    writer = _real_writer(tmp_path)
    sink = FakeSink({"/imu": writer})
    publisher = MqttPublisher(broker_url, tenant, robot)
    spool = Spool(tmp_path / "spool")
    service = FleetService(
        sink=sink,
        streams=_streams(flush_interval_s=flush_interval_s, rate_hz=rate_hz),
        publisher=publisher,
        spool=spool,
    )
    return writer, spool, service


def _speed_up_heartbeat(monkeypatch: pytest.MonkeyPatch, interval_s: float) -> None:
    """Make FleetService's heartbeat tick every `interval_s` instead of the 30s default.

    HeartbeatThread.run() fires its FIRST tick immediately, but that tick
    races StreamRouter's own connect (gated behind at least one
    `queue.drain` timeout) -- against a real broker the router usually
    hasn't connected yet, so that first heartbeat is spooled, not published
    live (the heartbeat lane is never replayed -- a known step-4 design
    gap, see the plan's progress ledger). Shortening the interval here
    doesn't change any behavior under test; it just keeps the *next* tick
    (which will find the router online) from being 30 real seconds away.
    """
    import importlib

    # importlib.import_module (== `from src.sink.publish.service import
    # FleetService`'s own resolution path) always checks sys.modules for the
    # full dotted name first. `from src.sink.publish import service` instead
    # resolves via getattr(src.sink.publish, "service") -- normally the same
    # object, but test_service.py's own eager-import test
    # (monkeypatch.delitem + reimport, restored via sys.modules on teardown)
    # can leave that package attribute pointing at an orphaned duplicate
    # module distinct from the one sys.modules -- and hence FleetService --
    # actually uses, silently patching the wrong object.
    heartbeat_module = importlib.import_module("src.sink.publish.heartbeat")
    service_module = importlib.import_module("src.sink.publish.service")

    monkeypatch.setattr(
        service_module,
        "HeartbeatThread",
        functools.partial(heartbeat_module.HeartbeatThread, interval_s=interval_s),
    )


# -- 1. full path: real writer -> real FleetService -> real broker -> subscriber --------


EXPECTED_IMU_SCHEMA = {
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

EXPECTED_HEARTBEAT_KEYS = {
    "v",
    "t",
    "online",
    "bagel_version",
    "uptime_s",
    "subscriptions",
    "channels_active",
    "queue",
    "spool",
    "disk_free_bytes",
    "cert_expires_at",
    "reconnects",
}


def _assert_live_schema_and_heartbeat(
    sub: _Subscriber, schema_topic: str, heartbeat_topic: str
) -> None:
    # Live delivery: payload only -- a retain=True publish delivered to an
    # already-subscribed client carries retain=0 on the wire (MQTT semantics;
    # the flag marks "delivered because of a NEW subscription", not "this
    # topic happens to be retained").
    schema = _drain(sub.inbox, schema_topic)
    assert schema is not None, "schema never arrived"
    assert json.loads(schema[1]) == EXPECTED_IMU_SCHEMA

    heartbeat = _drain(sub.inbox, heartbeat_topic, timeout_s=15.0)
    assert heartbeat is not None, "heartbeat never arrived"
    assert json.loads(heartbeat[1])["online"] is True


def _assert_retained_for_late_subscriber(
    late: _Subscriber, schema_topic: str, heartbeat_topic: str
) -> None:
    """A subscriber joining AFTER schema/heartbeat were published gets them
    immediately, with retain=1 -- step 3's own retained-delivery pattern.
    `late` must already be connected (subscribed AFTER those publishes).

    The §3 heartbeat key-set/shape checks live here rather than on the live
    tick: this is the durable, retained copy a late-joining reader (a real
    MCP tool included) actually sees, so it's the more meaningful place to
    pin the wire shape down.
    """
    late_schema = _drain(late.inbox, schema_topic)
    assert late_schema is not None and late_schema[2] is True
    assert json.loads(late_schema[1]) == EXPECTED_IMU_SCHEMA

    late_heartbeat = _drain(late.inbox, heartbeat_topic)
    assert late_heartbeat is not None and late_heartbeat[2] is True
    hb = json.loads(late_heartbeat[1])
    assert hb["online"] is True
    assert set(hb) == EXPECTED_HEARTBEAT_KEYS
    assert hb["cert_expires_at"] is None
    assert set(hb["spool"]) == {"bytes", "pending", "evicted"}


# Each wait below is paced by a real flush_interval_s (2s) wall-clock cycle
# that only advances when StreamRouter's background thread actually gets
# scheduled. `_drain`'s timeout is a ceiling, not a fixed sleep -- it returns
# the instant the event arrives -- so a generous one costs nothing in the
# common case; it only matters under heavy concurrent load (observed once:
# stacked immediately after two other full e2e suite runs on a loaded dev
# machine, the second-flush wait below missed the OLD 10s default ceiling
# -- 5x the flush interval, evidently not enough headroom under that load --
# while 5/5 isolated reruns of the same test passed cleanly straight after,
# confirming a test-side margin issue, not a RouterCore/StreamRouter/Spool
# defect: nothing about the assertions changed, only the ceiling below did).
_BATCH_WAIT_S = 30.0


def _assert_rate_capped_batches_ascend(
    writer: "TopicBufferWriter", sub: _Subscriber, topic: str
) -> None:
    # Canary flush: whatever unpredictable delay preceded this call (schema/
    # heartbeat drains, a late-subscriber round trip) has left the router's
    # flush_interval_s clock at an unknown phase. A batch arriving via the
    # subscriber proves a flush just happened -- i.e. that clock just reset
    # -- so the burst below gets a full, uninterrupted flush window instead
    # of racing an unknown one (the failure mode this canary replaced: an
    # in-progress flush interleaving with the 4 appends below and splitting
    # the same-slot samples across two batches).
    writer.append({"x": 0.0, "timestamp_seconds": 900.0})
    canary = _drain(sub.inbox, topic, timeout_s=_BATCH_WAIT_S)
    assert canary is not None, "canary channels batch never arrived"
    canary_seq = json.loads(canary[1])["seq"]

    # 3 samples land in the same 1Hz slot (rate-capped to the last one
    # written); a 4th lands in the next slot -- one batch, 2 samples.
    writer.append({"x": 1.0, "timestamp_seconds": 1_000.0})
    writer.append({"x": 2.0, "timestamp_seconds": 1_000.4})
    writer.append({"x": 3.0, "timestamp_seconds": 1_000.9})
    writer.append({"x": 4.0, "timestamp_seconds": 1_001.2})

    batch = _drain(sub.inbox, topic, timeout_s=_BATCH_WAIT_S)
    assert batch is not None, "channels batch never arrived"
    body = json.loads(batch[1])
    assert body["v"] == 1
    assert body["seq"] == canary_seq + 1
    # rate-capped: 4 raw appends -> 2 samples (one per 1Hz slot), sorted by t;
    # the slot-1000 winner is the LAST of the 3 samples raced into it.
    assert [s["c"] for s in body["samples"]] == ["imu.x", "imu.x"]
    assert [s["v"] for s in body["samples"]] == [3.0, 4.0]

    # a second flush cycle: seq must be ascending.
    writer.append({"x": 5.0, "timestamp_seconds": 1_002.5})
    batch2 = _drain(sub.inbox, topic, timeout_s=_BATCH_WAIT_S)
    assert batch2 is not None, "second channels batch never arrived"
    assert json.loads(batch2[1])["seq"] == canary_seq + 2


def test_full_path_streams_batches_schema_and_heartbeat(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parsed = urlparse(BROKER)
    tenant, robot = "e2e", uuid.uuid4().hex[:8]
    host, port = parsed.hostname, parsed.port or 1883
    schema_topic = wire_topic(tenant, robot, "schema")
    heartbeat_topic = wire_topic(tenant, robot, "heartbeat")
    channels_topic = wire_topic(tenant, robot, "channels")

    _speed_up_heartbeat(monkeypatch, interval_s=0.3)
    sub = _Subscriber(host, port, tenant, robot)
    sub.connect()
    try:
        # flush_interval_s is generous (2s, not step 3's usual sub-second
        # values): the rate-cap assertion below needs a flush window wide
        # enough to comfortably outlast the canary round trip (publish +
        # QoS-1 ack + broker delivery to the subscriber) plus 4 real
        # file-locked writer.append() calls, with margin for CI jitter --
        # see _assert_rate_capped_batches_ascend's canary-sync comment.
        writer, spool, service = _build_service(
            tmp_path, BROKER, tenant, robot, flush_interval_s=2.0, rate_hz=1.0
        )
        service.start()
        try:
            _assert_live_schema_and_heartbeat(sub, schema_topic, heartbeat_topic)

            late = _Subscriber(host, port, tenant, robot)
            late.connect()
            try:
                _assert_retained_for_late_subscriber(late, schema_topic, heartbeat_topic)
            finally:
                late.close()

            _assert_rate_capped_batches_ascend(writer, sub, channels_topic)

            assert _wait_until(lambda: spool.stats()["channels"].pending == 0, timeout_s=15.0), (
                "spool never drained the acked batches"
            )
        finally:
            service.stop()
    finally:
        sub.close()


# -- 2. chaos: kill the broker mid-stream, keep appending, restart it -------------------


def _run_outage_appends(writer: "TopicBufferWriter", start_t: float, duration_s: float) -> None:
    """Keep appending through the outage; every append must return immediately.

    `t` steps by 0.5s per sample -- comfortably past the 50Hz (max allowed
    `rate_hz`) slot width of 0.02s, so every sample lands in its own slot and
    none are dropped by the rate cap (irrelevant to what this test proves).
    """
    t = start_t
    value = 0.0
    deadline = time.monotonic() + duration_s
    while time.monotonic() < deadline:
        t += 0.5
        value += 1.0
        started = time.perf_counter()
        writer.append({"x": value, "timestamp_seconds": t})
        elapsed = time.perf_counter() - started
        assert elapsed < 1.0, f"append() blocked for {elapsed:.2f}s while the broker was down"
        time.sleep(0.1)


def _collect_seqs(sub: _Subscriber, topic: str, want_through: int, timeout_s: float) -> list[int]:
    """Collect delivered seqs until `want_through` has been seen or the deadline passes."""
    seqs: list[int] = []
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        got = _drain(sub.inbox, topic, timeout_s=1.0)
        if got is None:
            continue
        seqs.append(json.loads(got[1])["seq"])
        if seqs[-1] >= want_through:
            break
    return seqs


@requires_managed_broker
def test_chaos_kill_and_restart_broker_drains_spool_in_order(tmp_path: pathlib.Path) -> None:
    broker = _ChaosBroker(CHAOS_PORT)
    broker.start()
    try:
        broker_url = f"mqtt://localhost:{CHAOS_PORT}"
        tenant, robot = "chaos", uuid.uuid4().hex[:8]
        channels_topic = wire_topic(tenant, robot, "channels")

        sub = _Subscriber("localhost", CHAOS_PORT, tenant, robot)
        sub.connect()
        try:
            writer, spool, service = _build_service(
                tmp_path, broker_url, tenant, robot, flush_interval_s=0.1, rate_hz=50.0
            )
            service.start()
            try:
                # normal operation before chaos: prove the happy path first.
                writer.append({"x": 0.0, "timestamp_seconds": 500_000.0})
                first = _drain(sub.inbox, channels_topic, timeout_s=10.0)
                assert first is not None, "no batch before the outage even started"
                first_seq = json.loads(first[1])["seq"]
                assert _wait_until(lambda: spool.stats()["channels"].pending == 0, timeout_s=15.0)

                broker.kill()
                sub.prepare_for_outage()

                # keep appending through the outage: the tap must never block,
                # and the spool must keep absorbing new batches while offline.
                _run_outage_appends(writer, start_t=500_001.0, duration_s=4.0)
                assert _wait_until(lambda: spool.stats()["channels"].pending > 0, timeout_s=15.0), (
                    "spool pending never grew while the broker was down"
                )

                # Restart the broker and race this subscriber's reconnect
                # against it concurrently -- not sequentially -- so no extra
                # latency stacks onto the recovery window StreamRouter's own
                # (much slower, backoff-gated) reconnect is racing against.
                restart_thread = threading.Thread(target=broker.restart)
                restart_thread.start()
                sub.reconnect_now()
                restart_thread.join()

                assert _wait_until(
                    lambda: spool.stats()["channels"].pending == 0, timeout_s=60.0
                ), "spool never drained its backlog after the broker came back"
                final_last_seq = spool.stats()["channels"].last_seq

                # `first` (seq `first_seq`, pre-chaos) was already drained above
                # and never re-delivered (QoS-1 non-retained, already acked
                # before the outage) -- fold it back in so the seq range checked
                # below covers every batch this run ever produced, not just the
                # ones recovered after the restart.
                seqs_seen = [
                    first_seq,
                    *_collect_seqs(
                        sub, channels_topic, want_through=final_last_seq, timeout_s=20.0
                    ),
                ]
                unique_sorted = sorted(set(seqs_seen))
                assert unique_sorted == list(range(first_seq, final_last_seq + 1)), (
                    f"gap or missing seq: delivered {unique_sorted}, expected "
                    f"{first_seq}..{final_last_seq}"
                )
                # order check: consecutive-duplicate-collapsed delivery must be
                # exactly the ascending run (at-least-once permits re-delivery,
                # never reordering or a gap).
                deduped = [s for i, s in enumerate(seqs_seen) if i == 0 or s != seqs_seen[i - 1]]
                assert deduped == unique_sorted
            finally:
                service.stop()
        finally:
            sub.close()
    finally:
        broker.cleanup()


# -- 3. service.stop() publishes the stopped heartbeat before disconnect ----------------


def test_stop_publishes_stopped_heartbeat_before_disconnect(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parsed = urlparse(BROKER)
    tenant, robot = "stop", uuid.uuid4().hex[:8]
    heartbeat_topic = wire_topic(tenant, robot, "heartbeat")

    # A longer interval than test 1's -- this test needs exactly ONE "online"
    # heartbeat to land before stop() is called (a periodic tick landing
    # between the "online" drain and stop() would otherwise be picked up by
    # the "stopped" drain instead), while still tolerating the first tick
    # losing its race against the router's connect (see _speed_up_heartbeat).
    _speed_up_heartbeat(monkeypatch, interval_s=3.0)
    sub = _Subscriber(parsed.hostname, parsed.port or 1883, tenant, robot)
    sub.connect()
    try:
        _writer, _spool, service = _build_service(
            tmp_path, BROKER, tenant, robot, flush_interval_s=0.2, rate_hz=1.0
        )
        service.start()
        online = _drain(sub.inbox, heartbeat_topic, timeout_s=15.0)
        assert online is not None and json.loads(online[1])["online"] is True

        service.stop()

        stopped = _drain(sub.inbox, heartbeat_topic)
        assert stopped is not None, "clean stop never published a stopped heartbeat"
        body = json.loads(stopped[1])
        assert body["online"] is False
        assert body["reason"] == "stopped"

        # nothing else may follow: the stopped heartbeat must be the LAST word.
        extra = _drain(sub.inbox, heartbeat_topic, timeout_s=1.0)
        assert extra is None, f"unexpected heartbeat after stop(): {extra}"
    finally:
        sub.close()
