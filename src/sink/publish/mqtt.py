"""MqttPublisher: one robot's QoS-1 MQTT session to a fleet broker.

paho is imported lazily (via _paho) so this module never trips the
package's no-eager-import invariant; require_fleet() remains the gate.
"""

import json
import logging
import time
import weakref
from urllib.parse import urlparse

from src.sink.publish import require_fleet
from src.sink.publish.publisher import LWT_PAYLOAD, Publisher, PublishError, wire_topic

_DEFAULT_PORTS = {"mqtts": 8883, "mqtt": 1883}


def _paho() -> object:
    """Import and return the paho MQTT client module (lazy; test seam)."""
    import paho.mqtt.client as paho_client

    return paho_client


def _dump(payload: dict) -> str:
    """Serialize a payload dict to compact JSON."""
    return json.dumps(payload, separators=(",", ":"))


def _finalize(client: object) -> None:
    """Best-effort teardown for a garbage-collected publisher's client."""
    if client is None:
        return
    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        logging.debug("Best-effort teardown of an abandoned MQTT client failed")


class MqttPublisher(Publisher):
    """Publish fleet messages over MQTT with mTLS options and a retained last-will."""

    def __init__(  # noqa: PLR0913
        self,
        broker_url: str,
        tenant: str,
        robot: str,
        *,
        tls_ca_certs: str | None = None,
        tls_certfile: str | None = None,
        tls_keyfile: str | None = None,
        username: str | None = None,
        password: str | None = None,
        keepalive_s: int = 30,
    ) -> None:
        """Parse `broker_url` and stash connection options; does not connect.

        Args:
            broker_url: `mqtts://host[:port]` (TLS, default port 8883) or
                `mqtt://host[:port]` (plain, default port 1883).
            tenant: Tenant id used in the wire topic namespace.
            robot: Robot id used in the wire topic namespace and client id.
            tls_ca_certs: Path to a CA bundle for TLS.
            tls_certfile: Path to a client certificate for mTLS.
            tls_keyfile: Path to a client private key for mTLS.
            username: Broker username, if auth is required.
            password: Broker password, if auth is required.
            keepalive_s: MQTT keepalive interval in seconds.

        Raises:
            ValueError: If `broker_url`'s scheme is not `mqtt` or `mqtts`, or it has
                no host.

        """
        parsed = urlparse(broker_url)
        if parsed.scheme not in _DEFAULT_PORTS or not parsed.hostname:
            raise ValueError(
                f"broker_url scheme must be mqtt:// or mqtts:// with a host: {broker_url!r}"
            )
        self._host = parsed.hostname
        self._port = parsed.port or _DEFAULT_PORTS[parsed.scheme]
        self._use_tls = parsed.scheme == "mqtts" or any((tls_ca_certs, tls_certfile, tls_keyfile))
        self._tenant = tenant
        self._robot = robot
        self._tls = {"ca_certs": tls_ca_certs, "certfile": tls_certfile, "keyfile": tls_keyfile}
        self._username = username
        self._password = password
        self._keepalive_s = keepalive_s
        self._client: object = None
        # Set while our own close() is tearing the client down, so the
        # synchronous on_disconnect fire that a clean disconnect() triggers
        # (see _on_disconnect docstring) doesn't count as a reconnect event.
        self._closing = False
        self.reconnects = 0
        self._finalizer = weakref.finalize(self, _finalize, None)

    def set_tls(
        self,
        *,
        tls_ca_certs: str | None,
        tls_certfile: str | None,
        tls_keyfile: str | None,
    ) -> None:
        """Atomically replace the TLS material used by the NEXT `connect()`.

        Assigns a brand-new dict to `self._tls` rather than mutating the
        existing one in place -- a single reference assignment is safe
        against a background thread (e.g. `StreamRouter`'s reconnect loop)
        reading `self._tls` mid-`connect()`; it always sees either the
        fully-old or the fully-new dict, never a half-updated mix. This
        exists for the certificate-renewal path (see `identity.renew()`'s
        docstring): once a renewal has rotated the cert/key files on disk,
        this is the seam that tells the LIVE publisher about the new paths
        before its next reconnect, so it doesn't keep trying (and failing)
        to load the now-deleted old files.

        Does not itself force a reconnect or touch an already-open
        connection -- the currently connected client, if any, keeps running
        on the old material until its own next `connect()` call.
        """
        self._tls = {
            "ca_certs": tls_ca_certs,
            "certfile": tls_certfile,
            "keyfile": tls_keyfile,
        }

    def connect(self) -> None:
        """Build the paho client, arm the last-will, and connect.

        Enforces the fleet gate first, so a disabled or not-installed fleet
        subsystem never imports paho or opens a socket. If already connected
        to a prior client, tears it down before building the replacement
        (connect() is idempotent-with-replacement).
        """
        require_fleet()
        if self._client is not None:
            # Same _closing gate as close(): tearing down the prior client
            # here is our own doing, not an unexpected drop. If the prior
            # client's socket is still live (e.g. we are replacing it after
            # a wait_for_publish timeout, not a broker-side drop), paho can
            # fire on_disconnect synchronously from this teardown, and it
            # must not count as a reconnect.
            self._closing = True
            try:
                _finalize(self._client)
            finally:
                self._closing = False
        paho = _paho()
        client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=f"bagel-{self._tenant}-{self._robot}",
            protocol=paho.MQTTv5,
        )
        client.will_set(
            wire_topic(self._tenant, self._robot, "heartbeat"),
            _dump(LWT_PAYLOAD),
            qos=1,
            retain=True,
        )
        if self._use_tls:
            client.tls_set(**self._tls)
        if self._username is not None:
            client.username_pw_set(self._username, self._password)
        client.on_disconnect = self._on_disconnect
        client.connect(self._host, self._port, self._keepalive_s)
        client.loop_start()
        self._client = client
        # Re-register the finalizer against the live client, holding no ref to self.
        self._finalizer.detach()
        self._finalizer = weakref.finalize(self, _finalize, client)

    def _on_disconnect(
        self,
        client: object,
        userdata: object,
        disconnect_flags: object,
        reason_code: object,
        properties: object = None,
    ) -> None:
        """Paho VERSION2 on_disconnect hook: `(client, userdata, flags, reason, props)`.

        Verified against the installed paho 2.1.0 source
        (paho/mqtt/client.py, CallbackOnDisconnect_v2 / _do_on_disconnect):
        the VERSION2 callback always receives exactly these five positional
        args. Confirmed paho fires this callback even for a deliberate,
        clean close() -- our close() calls loop_stop() (which nulls the
        client's background thread) then disconnect(); with no background
        thread running, disconnect()'s outgoing DISCONNECT packet is written
        synchronously on the calling thread, and paho's packet-write path
        invokes on_disconnect right there. So a clean close is
        indistinguishable from an unexpected drop unless we gate on it
        ourselves: self._closing (set for the duration of our close()) is
        that gate, so `reconnects` counts only disconnects we didn't ask for.

        Never raises: a broker-thread callback that raises would only get
        logged and swallowed by paho itself, so any exception here is
        caught rather than relying on that.
        """
        try:
            if not self._closing:
                self.reconnects += 1
        except Exception:
            logging.debug("on_disconnect handler failed", exc_info=True)

    @property
    def connected(self) -> bool:
        """Return whether the underlying paho client reports itself connected."""
        return bool(self._client is not None and self._client.is_connected())

    def publish(
        self, kind: str, payload: dict, *, retain: bool = False, timeout_s: float = 10.0
    ) -> None:
        """Publish `payload` as JSON at QoS 1 on the wire topic for `kind`."""
        if self._client is None:
            raise PublishError("publisher is not connected")
        topic = wire_topic(self._tenant, self._robot, kind)
        info = self._client.publish(topic, _dump(payload), qos=1, retain=retain)
        try:
            info.wait_for_publish(timeout=timeout_s)
        except Exception as exc:
            raise PublishError(f"{topic}: not acknowledged within {timeout_s}s: {exc}") from exc
        if info.rc != 0 or not info.is_published():
            raise PublishError(f"{topic}: publish failed (rc={info.rc})")

    def close(self) -> None:
        """Publish a clean-stop heartbeat (best-effort) then stop and disconnect.

        Idempotent: safe to call more than once, and safe when never connected.
        """
        client, self._client = self._client, None
        if client is None:
            return
        self._closing = True
        try:
            if client.is_connected():
                stopped = {"v": 1, "t": time.time(), "online": False, "reason": "stopped"}
                try:
                    info = client.publish(
                        wire_topic(self._tenant, self._robot, "heartbeat"),
                        _dump(stopped),
                        qos=1,
                        retain=True,
                    )
                    info.wait_for_publish(timeout=5.0)
                except Exception:
                    logging.debug("Clean-stop heartbeat publish failed; closing anyway")
            client.loop_stop()
            client.disconnect()
        finally:
            self._closing = False
