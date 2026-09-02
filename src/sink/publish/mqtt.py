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
        self._finalizer = weakref.finalize(self, _finalize, None)

    def connect(self) -> None:
        """Build the paho client, arm the last-will, and connect.

        Enforces the fleet gate first, so a disabled or not-installed fleet
        subsystem never imports paho or opens a socket. If already connected
        to a prior client, tears it down before building the replacement
        (connect() is idempotent-with-replacement).
        """
        require_fleet()
        if self._client is not None:
            _finalize(self._client)
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
        client.connect(self._host, self._port, self._keepalive_s)
        client.loop_start()
        self._client = client
        # Re-register the finalizer against the live client, holding no ref to self.
        self._finalizer.detach()
        self._finalizer = weakref.finalize(self, _finalize, client)

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
