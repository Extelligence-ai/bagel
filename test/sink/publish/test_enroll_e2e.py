"""First-boot enroll-to-stream end-to-end (spec S6, step 6).

Gated on MQTT_TEST_BROKER (a real mosquitto broker), like
test_mqtt_integration.py and test_stream_e2e.py. Proves the full loop with NO
real cloud:

1. An in-thread fake enrollment server (``http.server``) backed by a REAL
   throwaway CA (``cryptography``) signs the robot's actual CSR -- the PEMs
   ``enroll()`` writes to disk are real, parseable certs, not string
   fixtures (``_build_ca_and_robot_cert`` below is adapted from
   test_identity.py's helper of the same name). Its response's
   ``broker_url`` is the dev-insecure path (``mqtt://<broker
   host>:<port>`` of the very MQTT_TEST_BROKER this suite already
   requires) so no second, TLS-terminating fake broker is needed to prove
   the loop end-to-end.
2. ``maybe_enroll_on_first_boot()`` (settings monkeypatched:
   FLEET_ENROLL_TOKEN/URL, FLEET_IDENTITY_DIRECTORY=tmp,
   FLEET_DEV_INSECURE=1) writes the identity to disk (0600 key) exactly as
   server.py's boot sequence does, before any manifest wiring runs.
3. Manifest-driven startup: ``startup._start_fleet`` is driven DIRECTLY
   with a manually built ``subscribed`` list -- not ``startup.start()``'s
   full ``subscriptions:`` loop -- because none of this repo's
   host-runnable ``TopicSink`` types (``ros1.bridge``, ``ros2.bridge``,
   ``mqtt``) can be stood up here without extra infrastructure this suite
   doesn't have: the two ROS bridges need an actual bridge process, and the
   ``mqtt`` sink's own ``#``-wildcard topic discovery is time-boxed and
   would race the deterministic sample-append assertions below instead of
   proving anything extra. Step 5's own e2e (test_stream_e2e.py)
   established the same real-``TopicBufferWriter``-backed ``FakeSink``
   double as the standard way to drive the tap -> RouterCore ->
   StreamRouter -> Spool -> MqttPublisher pipeline for real without a real
   upstream sink -- ``_start_fleet`` cannot itself tell a real sink from
   this one; it only ever touches ``sink.subscribed_topics`` and
   ``sink._buffers``, which this double provides identically to a real
   ``TopicSink``. This is the plan's explicitly authorized fallback: the
   enrolled identity, its on-disk files, and the MQTT broker/wire traffic
   are all real -- only the "who feeds the buffer" side is a double.
4. Appending samples to that writer and reading them back off the SAME
   real broker's ``bagel/v1/<tenant>/<robot>/channels`` topic proves the
   fresh identity's broker_url/tenant/robot_id actually drive a live
   MqttPublisher; the retained heartbeat's non-null ``cert_expires_at`` is
   the first e2e in this repo where that value isn't None (step 5's e2e
   never had an identity to carry one).
5. Renewal-501-grace (a separate test): ``identity.renew()`` against a fake
   renew server that always answers 501 (server does not offer renewal
   yet) returns None, records the attempt, and never raises -- and proves
   the enroll response's OPTIONAL ``renew_url`` field (see identity.py's
   module docstring) is the URL ``renew()`` actually targets, by pointing
   it at a DIFFERENT host than the enroll server.

Locally:
    docker run -d -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
    MQTT_TEST_BROKER=mqtt://localhost:1883 uv run pytest test/sink/publish/test_enroll_e2e.py
"""

import datetime
import functools
import http.server
import json
import os
import pathlib
import queue
import stat
import threading
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa
import pytest
import yaml

from settings import settings
from src.sink import startup
from src.sink.publish import identity as identity_mod
from src.sink.publish.publisher import wire_topic

if TYPE_CHECKING:
    from src.sink.buffer import TopicBufferWriter

BROKER = os.environ.get("MQTT_TEST_BROKER")

pytestmark = pytest.mark.skipif(not BROKER, reason="MQTT_TEST_BROKER not set")

ENROLL_TOKEN = "e2e-first-boot-token"  # noqa: S105 -- test fixture, not a real secret
IMU_STRUCT = pa.struct([pa.field("x", pa.float64())])
EXPIRES_AT = "2027-06-15T00:00:00Z"

Inbox = "queue.Queue[tuple[str, bytes, bool]]"


# -- small helpers (adapted from test_stream_e2e.py: no bare sleeps, deadline polling) --


def _wait_until(predicate: Callable[[], bool], timeout_s: float, interval_s: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return predicate()


def _drain(inbox: Inbox, topic: str, timeout_s: float = 10.0) -> tuple[str, bytes, bool] | None:
    """Pull from `inbox` until `topic` matches or the deadline passes."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            got = inbox.get(timeout=deadline - time.time())
        except queue.Empty:
            return None
        if got[0] == topic:
            return got
    return None


class FakeSink:
    """Minimal TopicSink double: the `buffer_writer`/`subscribed_topics` seam FleetService uses.

    Adapted from test_stream_e2e.py's double of the same name -- duplicated
    rather than imported across test modules (no existing precedent for
    that in this package; see this module's docstring, point 3). Kept in
    lockstep with FleetService's seam: it originally exposed the raw
    `_buffers` attribute the service then reached into, and broke silently
    (this file is broker-gated, so nothing ran it) when the service moved to
    the `buffer_writer()` accessor.
    """

    def __init__(self, buffers: dict[str, "TopicBufferWriter"]) -> None:
        self._buffers = buffers

    @property
    def subscribed_topics(self) -> list[str]:
        return list(self._buffers)

    def buffer_writer(self, topic: str) -> "TopicBufferWriter":
        return self._buffers[topic]


def _real_writer(tmp_path: pathlib.Path, topic: str = "/imu") -> "TopicBufferWriter":
    """A real TopicBufferWriter (not a fake) backed by a tmp cache."""
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


class _Subscriber:
    """One paho client subscribed to a robot's whole `bagel/v1/<tenant>/<robot>/#` tree.

    A trimmed adaptation of test_stream_e2e.py's `_Subscriber` -- this file
    has no chaos scenario, so the outage/reconnect machinery there is
    dropped; connect/drain-via-inbox/close is all this test needs.
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

    def close(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()


def _build_ca_and_robot_cert(common_name: str, robot_public_key: object) -> tuple[bytes, bytes]:
    """Build a throwaway self-signed CA and a robot cert it signs, both as PEM.

    Adapted from test_identity.py's helper of the same name (see this
    module's docstring, point 1) -- returns (robot_cert_pem, ca_cert_pem).
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "e2e test fleet CA")])
    now = datetime.datetime.now(datetime.UTC)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    robot_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    robot_cert = (
        x509.CertificateBuilder()
        .subject_name(robot_name)
        .issuer_name(ca_name)
        .public_key(robot_public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=90))
        .sign(ca_key, hashes.SHA256())
    )

    return (
        robot_cert.public_bytes(serialization.Encoding.PEM),
        ca_cert.public_bytes(serialization.Encoding.PEM),
    )


def _make_enroll_handler(
    *,
    broker_url: str,
    renew_url: str,
    tenant: str,
    robot_id: str,
    expires_at: str,
) -> type[http.server.BaseHTTPRequestHandler]:
    """Build a fake `POST /v1/enroll` handler that signs the REAL posted CSR.

    Every field of the closure is fixed per test scenario -- unlike
    test_identity.py's fake server (which switches canned responses off the
    request token), this e2e only ever needs one always-succeeds shape, with
    the response fields the test cares about (broker_url/renew_url/tenant/
    robot_id/expires_at) parameterized per call.
    """

    class _Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            pass  # silence the default stderr access log during tests

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw)
            csr_pem = payload["csr_pem"].encode()

            from cryptography import x509

            csr = x509.load_pem_x509_csr(csr_pem)
            robot_cert_pem, ca_cert_pem = _build_ca_and_robot_cert(
                f"{tenant}-{robot_id}", csr.public_key()
            )
            body = json.dumps(
                {
                    "cert_pem": robot_cert_pem.decode(),
                    "ca_pem": ca_cert_pem.decode(),
                    "broker_url": broker_url,
                    "tenant": tenant,
                    "robot_id": robot_id,
                    "expires_at": expires_at,
                    "renew_url": renew_url,
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return _Handler


class _AlwaysRenewUnavailableHandler(http.server.BaseHTTPRequestHandler):
    """POST /v1/renew -- always 501 (server does not offer renewal on this deployment)."""

    def log_message(self, fmt: str, *args: object) -> None:
        pass  # silence the default stderr access log during tests

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)  # drain the request body
        body = b"renewal not enabled on this deployment"
        self.send_response(501)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _start_http_server(
    handler_cls: type[http.server.BaseHTTPRequestHandler],
) -> tuple[http.server.HTTPServer, threading.Thread]:
    server = http.server.HTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _stop_http_server(server: http.server.HTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def _speed_up_heartbeat(monkeypatch: pytest.MonkeyPatch, interval_s: float) -> None:
    """Make FleetService's heartbeat tick every `interval_s` instead of the 30s default.

    Adapted from test_stream_e2e.py's helper of the same name/purpose: the
    FIRST heartbeat tick fires immediately but races StreamRouter's own
    connect, so against a real broker it is usually spooled rather than
    published live -- and the heartbeat lane is never replayed (a known
    step-4 design gap). Without this, the retained heartbeat this test's
    late subscriber waits on wouldn't land until the next tick, 30 real
    seconds away. `_start_fleet` builds its own `FleetService` internally
    (this test never constructs one directly), but `FleetService`'s module
    namespace is what actually gets patched here, so it applies regardless
    of which caller builds the instance.
    """
    import importlib

    heartbeat_module = importlib.import_module("src.sink.publish.heartbeat")
    service_module = importlib.import_module("src.sink.publish.service")

    monkeypatch.setattr(
        service_module,
        "HeartbeatThread",
        functools.partial(heartbeat_module.HeartbeatThread, interval_s=interval_s),
    )


# -- 1. full first-boot loop: enroll (real CA/CSR) -> identity on disk -> stream ---------


@pytest.fixture(autouse=True)
def _isolated_fleet_service() -> None:
    """Clear the module-level FleetService holder before/after every test here.

    Mirrors test_startup_streams.py's fixture of the same purpose: a leaked,
    still-running FleetService from a prior test (or a failed one) must
    never bleed into the next.
    """
    startup._FLEET_SERVICE = None
    yield
    if startup._FLEET_SERVICE is not None:
        startup._FLEET_SERVICE.stop()
        startup._FLEET_SERVICE = None


def _first_boot_enroll(  # noqa: PLR0913 -- one field per scenario input, kept explicit for the caller
    *,
    identity_directory: pathlib.Path,
    enroll_url: str,
    tenant: str,
    robot_id: str,
    broker_url: str,
    renew_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> identity_mod.Identity:
    """Run step 1: monkeypatch settings, call `maybe_enroll_on_first_boot()`, assert on disk."""
    monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", ENROLL_TOKEN)
    monkeypatch.setattr(settings, "FLEET_ENROLL_URL", enroll_url)
    monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(identity_directory))
    monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
    monkeypatch.setattr(settings, "FLEET_ENABLED", True)

    identity_mod.maybe_enroll_on_first_boot()

    assert identity_mod.is_enrolled(identity_directory) is True
    key_mode = stat.S_IMODE((identity_directory / "robot.key").stat().st_mode)
    assert key_mode == 0o600

    doc = yaml.safe_load((identity_directory / "identity.yaml").read_text())
    assert doc["broker_url"] == broker_url
    assert doc["expires_at"] == EXPIRES_AT
    assert doc["renew_url"] == renew_url  # the enroll response's renew_url landed on disk

    enrolled = identity_mod.load_identity(identity_directory)
    assert enrolled.tenant == tenant
    assert enrolled.robot_id == robot_id
    assert enrolled.renew_url == renew_url
    return enrolled


def _assert_batch_and_heartbeat(
    broker_host: str, broker_port: int, tenant: str, robot_id: str, writer: "TopicBufferWriter"
) -> None:
    """Run steps 3-4: append a sample, prove a live batch and a retained heartbeat arrive.

    The heartbeat is drained via `sub` FIRST (a live "online" delivery,
    retain=0 on the wire per MQTT semantics: it arrives because `sub` was
    already subscribed) -- ONLY once that has actually landed is it
    guaranteed to also be the broker's retained value for the topic, so
    `late`, connecting strictly after, is guaranteed a fresh SUBSCRIBE-time
    retained delivery (retain=1) rather than possibly racing the FIRST
    heartbeat tick, which can be spooled (not published) if it beats
    StreamRouter's own connect -- see `_speed_up_heartbeat`'s docstring.
    """
    channels_topic = wire_topic(tenant, robot_id, "channels")
    heartbeat_topic = wire_topic(tenant, robot_id, "heartbeat")

    sub = _Subscriber(broker_host, broker_port, tenant, robot_id)
    sub.connect()
    try:
        writer.append({"x": 1.0, "timestamp_seconds": 1_000.0})
        batch = _drain(sub.inbox, channels_topic, timeout_s=30.0)
        assert batch is not None, "channels batch never arrived"
        body = json.loads(batch[1])
        assert body["samples"][0]["c"] == "imu.x"
        assert body["samples"][0]["v"] == 1.0

        online = _drain(sub.inbox, heartbeat_topic, timeout_s=15.0)
        assert online is not None, "heartbeat never arrived"
        assert json.loads(online[1])["online"] is True
    finally:
        sub.close()

    # The retained heartbeat carries the enrolled cert's real expiry -- the
    # first e2e in this repo where cert_expires_at is non-null.
    late = _Subscriber(broker_host, broker_port, tenant, robot_id)
    late.connect()
    try:
        heartbeat = _drain(late.inbox, heartbeat_topic, timeout_s=15.0)
        assert heartbeat is not None and heartbeat[2] is True
        hb = json.loads(heartbeat[1])
        assert hb["online"] is True
        assert hb["cert_expires_at"] == EXPIRES_AT
    finally:
        late.close()


def test_first_boot_enroll_to_stream(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parsed = urlparse(BROKER)
    broker_host = parsed.hostname or "localhost"
    broker_port = parsed.port or 1883
    tenant = "e2e"
    robot_id = f"robot-{uuid.uuid4().hex[:8]}"
    broker_url = f"mqtt://{broker_host}:{broker_port}"

    renew_server, renew_thread = _start_http_server(_AlwaysRenewUnavailableHandler)
    enroll_server, enroll_thread = None, None
    try:
        renew_url = f"http://127.0.0.1:{renew_server.server_port}"
        enroll_handler_cls = _make_enroll_handler(
            broker_url=broker_url,
            renew_url=renew_url,
            tenant=tenant,
            robot_id=robot_id,
            expires_at=EXPIRES_AT,
        )
        enroll_server, enroll_thread = _start_http_server(enroll_handler_cls)

        _speed_up_heartbeat(monkeypatch, interval_s=0.5)

        # 1. First-boot enrollment: no real cloud, a real signed CSR.
        _first_boot_enroll(
            identity_directory=tmp_path / "identity",
            enroll_url=f"http://127.0.0.1:{enroll_server.server_port}",
            tenant=tenant,
            robot_id=robot_id,
            broker_url=broker_url,
            renew_url=renew_url,
            monkeypatch=monkeypatch,
        )

        # 2. Manifest-driven startup, real _start_fleet -- see module
        # docstring point 3 for why this drives _start_fleet directly
        # rather than startup.start()'s subscriptions: loop.
        writer = _real_writer(tmp_path)
        sink = FakeSink({"/imu": writer})
        manifest = {
            "streams": {
                "flush_interval_s": 0.3,
                "channels": [{"topic": "/imu", "fields": ["x"], "rate_hz": 5}],
            }
        }

        report = startup._start_fleet(manifest, [(sink, ["/imu"])])

        assert report == {"fleet": "started"}
        service = startup._FLEET_SERVICE
        assert service is not None
        assert service._identity is not None
        assert service._identity.robot_id == robot_id

        # 3-4. Append a sample; a live batch and a retained heartbeat with a
        # non-null cert_expires_at both arrive on the same real broker.
        _assert_batch_and_heartbeat(broker_host, broker_port, tenant, robot_id, writer)

        assert _wait_until(
            lambda: service._spool.stats()["channels"].pending == 0, timeout_s=15.0
        ), "spool never drained the acked batch"
    finally:
        if startup._FLEET_SERVICE is not None:
            startup._FLEET_SERVICE.stop()
            startup._FLEET_SERVICE = None
        if enroll_server is not None:
            _stop_http_server(enroll_server, enroll_thread)
        _stop_http_server(renew_server, renew_thread)


# -- 2. renewal-501-grace: cloud ships renewal disabled behind a flag -------------------


def test_renewal_against_server_without_renewal_offered_returns_none_with_no_crash(
    tmp_path: pathlib.Path,
) -> None:
    """Points the enrolled identity's OWN renew_url at a fake server that always 501s.

    Proves two things at once: (a) `renew()` honors an enroll response's
    OPTIONAL `renew_url` (a DIFFERENT host than the enroll server here,
    exactly like production's split enroll/renew topology -- see
    identity.py's module docstring), and (b) the default "renewal not
    offered yet" (501) grace path: no exception, the attempt is recorded,
    and the existing identity is left completely unchanged.
    """
    renew_server, renew_thread = _start_http_server(_AlwaysRenewUnavailableHandler)
    enroll_server, enroll_thread = None, None
    try:
        renew_url = f"http://127.0.0.1:{renew_server.server_port}"
        enroll_handler_cls = _make_enroll_handler(
            broker_url="mqtt://localhost:1",  # never dialed in this test
            renew_url=renew_url,
            tenant="e2e-renew",
            robot_id="robot-renew",
            expires_at=EXPIRES_AT,
        )
        enroll_server, enroll_thread = _start_http_server(enroll_handler_cls)
        enroll_url = f"http://127.0.0.1:{enroll_server.server_port}"

        directory = tmp_path / "identity"
        identity = identity_mod.enroll(ENROLL_TOKEN, enroll_url, directory)
        assert identity.renew_url == renew_url

        result = identity_mod.renew(identity)  # must not raise

        assert result is None
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        assert "last_renewal_attempt_at" in doc  # attempt recorded
        assert doc["expires_at"] == identity.expires_at  # unchanged -- not a success
        assert doc["key_file"] == identity.key_path.name  # pointer never moved
    finally:
        if enroll_server is not None:
            _stop_http_server(enroll_server, enroll_thread)
        _stop_http_server(renew_server, renew_thread)
