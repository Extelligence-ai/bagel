"""MqttPublisher: one robot's QoS-1 MQTT session to a fleet broker.

paho is imported lazily (via _paho) so this module never trips the
package's no-eager-import invariant; require_fleet() remains the gate.
"""

import json
import logging
import threading
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
        client_id_suffix: str = "",
        retain_messages: bool = True,
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
            client_id_suffix: Appended to the deterministic
                `bagel/{tenant}/{robot}` client id (see `connect()`).
                Defaults to `""` (the live service's id, unchanged). The
                selftest CLI passes `"/selftest"` here (Codex round 3
                follow-up, PR #214 P2 on comment 3925391258): the live
                service's `MqttPublisher` and the selftest's both derive the
                SAME client id from the same `(tenant, robot)` pair, and
                MQTT brokers kick the existing session when a NEW connection
                claims an already-connected client id -- so running the
                selftest against an enrolled robot's `mqtts://` broker,
                while that robot's real streaming service is also
                connected, would silently DISPLACE (disconnect) the live
                session. Cloud confirmed ACLs key on the cert CN, not the
                client id, so appending a suffix here is free -- it doesn't
                touch authorization. Still fully deterministic (no
                randomness): the same `(tenant, robot, suffix)` always
                produces the same id, preserving the reconnect-displacement
                semantics the id's own determinism exists for (see
                `connect()`'s comment) -- it just puts the selftest in its
                own, separate client-id namespace instead of the live
                service's. `"/"`, not `"-"` (Codex round 3 follow-up, PR
                #214 P2 on comment 3927231074): a hyphen suffix
                reintroduces exactly the hyphen-injectivity ambiguity the
                `"/"` delimiter between tenant/robot was already fixed for
                (see `connect()`'s comment) -- robot `"r7"` suffixed
                `"-selftest"` would collide with a robot actually NAMED
                `"r7-selftest"`. `"/"` is outside both id charsets (robot
                ids match `^[a-z0-9][a-z0-9_-]{0,62}$`), so it's provably
                collision-free the same way.
            retain_messages: When `False`, forces `retain=False` on every
                publish (schema, heartbeat, and `close()`'s clean-stop
                beat) and skips arming a last-will entirely (see
                `connect()`). Defaults to `True` (the live service's
                behavior, unchanged -- retained schema/heartbeat/LWT are
                load-bearing there: a
                late subscriber must be able to decode live batches and
                see current liveness without waiting for the next publish).
                The selftest CLI passes `False` (Codex round 3 follow-up,
                PR #214 P1 on comment 3927023413): it keeps publishing AS
                the robot (client-id suffix aside, cert-CN ACLs make an
                isolated identity a non-starter -- see `client_id_suffix`
                above), so its retained publishes would otherwise linger
                on the SAME shared robot topics with nothing to overwrite
                them once the run ends -- its fixture schema staying
                retained until the live service's next reconnect (a late
                subscriber decodes live batches against the WRONG schema
                in the meantime), and its `close()` beat leaving a
                retained `online: false` (the robot looks dead until the
                next live beat). Retention is not load-bearing for
                conformance -- the validator subscribes before/during the
                run, and the wire contract's §10 sequence is about
                ordering and shape, not retention -- so going fully
                non-retained costs the selftest nothing.

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
        self._client_id_suffix = client_id_suffix
        self._retain_messages = retain_messages
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
            # "/" is outside both id charsets (robot ids match
            # ^[a-z0-9][a-z0-9_-]{0,62}$; tenant ids are Cognito-derived
            # without "/"), so this delimiter is provably collision-free --
            # unlike the old "bagel-{tenant}-{robot}" hyphen join, where
            # ("acme-west", "r7") and ("acme", "west-r7") both produced
            # "bagel-acme-west-r7" (Codex review). Mirrors the cert CN's
            # delimiter. Must stay deterministic per (tenant, robot,
            # client_id_suffix): reconnect displacement semantics depend on
            # it. `client_id_suffix` defaults to "" (unchanged live-service
            # id); the selftest CLI passes "-selftest" so it never displaces
            # the live service's own session on the same broker (see
            # `__init__`'s docstring).
            client_id=f"bagel/{self._tenant}/{self._robot}{self._client_id_suffix}",
            protocol=paho.MQTTv5,
        )
        # No LWT at all when `retain_messages` is False (Codex round 3
        # follow-up, PR #214 P1 on comment 3927023413): `will_set` exists to
        # tell subscribers the ROBOT went offline unexpectedly, RETAINED so
        # a late subscriber still sees it -- neither half of that applies to
        # an ephemeral, non-retained session like the selftest's. It isn't
        # "the robot" going offline, just a diagnostic run ending, and
        # arming even a non-retained will would still momentarily publish a
        # confusing "offline" blip on the SHARED robot heartbeat topic to
        # anyone watching live if this session ever dropped uncleanly.
        # Simplest and safest: skip arming a will at all for this mode,
        # rather than merely un-retaining it.
        if self._retain_messages:
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
        """Publish `payload` as JSON at QoS 1 on the wire topic for `kind`.

        `retain` is forced to `False` when `self._retain_messages` is
        `False` regardless of what the caller (e.g. `Publisher.
        publish_schema`/`publish_heartbeat`, which always call with
        `retain=True`) requested -- see `__init__`'s `retain_messages`
        docstring for why the selftest needs this.
        """
        if self._client is None:
            raise PublishError("publisher is not connected")
        topic = wire_topic(self._tenant, self._robot, kind)
        effective_retain = retain and self._retain_messages
        info = self._client.publish(topic, _dump(payload), qos=1, retain=effective_retain)
        try:
            info.wait_for_publish(timeout=timeout_s)
        except Exception as exc:
            raise PublishError(f"{topic}: not acknowledged within {timeout_s}s: {exc}") from exc
        if info.rc != 0 or not info.is_published():
            raise PublishError(f"{topic}: publish failed (rc={info.rc})")

    def wait_for_retained_heartbeat(self, timeout_s: float = 1.5) -> dict | None:
        """Subscribe to this robot's own heartbeat topic; return a RETAINED payload, if any.

        A selftest-only probe (Codex round 3 follow-up, PR #214 P1, comment
        3927287968), NOT part of the `Publisher` ABC -- it's a `MqttPublisher`-
        specific capability, called via `getattr(publisher,
        "wait_for_retained_heartbeat", None)` from `selftest.run_selftest`
        (see its `_check_no_live_session`), so any `Publisher` implementation
        that doesn't offer it (including most test doubles) simply skips the
        check rather than needing to grow the ABC or stub out a method it has
        no use for. Design choice: keeping this as a small, direct-paho,
        selftest-scoped helper on `MqttPublisher` was cleaner than adding a
        new abstract method every `Publisher` implementation would have to
        carry, most of which (the live service's own usage) never need it.

        Subscribes at QoS 1, waits up to `timeout_s` for the FIRST message
        with its `retain` flag set (a broker delivers any existing retained
        message on that topic immediately upon subscribing -- before any
        live traffic), then unsubscribes and restores the client's prior
        `on_message` handler either way. A non-retained message (live
        traffic arriving in the same window) is ignored; it does not
        satisfy or extend the wait.

        Returns:
            The retained heartbeat's parsed JSON payload, or `None` if
            nothing retained arrived within `timeout_s` (a fresh
            robot/broker with nothing retained yet) or the retained
            payload failed to parse (treated the same as "unknown" --
            never raises here; the caller decides what "unknown" means).

        Raises:
            PublishError: not connected.

        """
        if self._client is None:
            raise PublishError("publisher is not connected")
        topic = wire_topic(self._tenant, self._robot, "heartbeat")
        result: dict[str, object] = {}
        done = threading.Event()

        def _on_retained_message(_client: object, _userdata: object, message: object) -> None:
            if done.is_set() or not message.retain:  # type: ignore[attr-defined]
                return
            try:
                result["payload"] = json.loads(message.payload.decode("utf-8"))  # type: ignore[attr-defined]
            except (json.JSONDecodeError, UnicodeDecodeError):
                result["payload"] = None
            done.set()

        previous_on_message = self._client.on_message
        self._client.on_message = _on_retained_message
        self._client.subscribe(topic, qos=1)
        try:
            done.wait(timeout=timeout_s)
        finally:
            self._client.unsubscribe(topic)
            self._client.on_message = previous_on_message
        return result.get("payload")  # type: ignore[return-value]

    def disconnect_without_publishing(self) -> None:
        """Tear down the connection WITHOUT publishing a clean-stop heartbeat.

        A selftest-only cleanup helper (Codex round 3 follow-up, PR #214
        P1, comment 3927287968), used when `run_selftest`'s live-session
        refusal (`wait_for_retained_heartbeat` found a connected live
        session) needs to release THIS session's deterministic client id
        before raising -- otherwise it lingers until this `MqttPublisher`
        is eventually garbage-collected, which is neither prompt nor
        reliable enough to guarantee a follow-up run can immediately
        reclaim the same client id cleanly.

        Deliberately NOT `close()`: a live session was just detected as
        connected, and even `close()`'s non-retained clean-stop beat (with
        `retain_messages=False`) would still deliver a misleading momentary
        `online: false` to whoever is watching THAT live session's own
        heartbeat topic -- exactly the class of problem this whole
        precondition exists to prevent. This tears the connection down
        silently: no publish of any kind.

        Idempotent and safe when never connected, same as `close()`.
        """
        client, self._client = self._client, None
        if client is None:
            return
        self._closing = True
        try:
            client.loop_stop()
            client.disconnect()
        finally:
            self._closing = False

    def close(self, reason: str = "stopped") -> None:
        """Publish a clean-stop heartbeat (best-effort) then stop and disconnect.

        Idempotent: safe to call more than once, and safe when never connected.

        Args:
            reason: Carried verbatim in the clean-stop heartbeat's `reason`
                field (spec §3). `FleetService.stop()` uses the default
                `"stopped"`; `FleetService.pause()` passes `"paused"` so a
                paused robot's last retained heartbeat reads distinctly from
                a genuinely stopped one. Retained only when
                `self._retain_messages` is `True` (the live service default)
                -- the selftest's clean-stop beat must not leave a retained
                `online: false` corpse on the shared robot's heartbeat
                topic (Codex round 3 follow-up, PR #214 P1 on comment
                3927023413), or the robot would look dead until its live
                service's next beat.

        """
        client, self._client = self._client, None
        if client is None:
            return
        self._closing = True
        try:
            if client.is_connected():
                stopped = {"v": 1, "t": time.time(), "online": False, "reason": reason}
                try:
                    info = client.publish(
                        wire_topic(self._tenant, self._robot, "heartbeat"),
                        _dump(stopped),
                        qos=1,
                        retain=self._retain_messages,
                    )
                    info.wait_for_publish(timeout=5.0)
                except Exception:
                    logging.debug("Clean-stop heartbeat publish failed; closing anyway")
            client.loop_stop()
            client.disconnect()
        finally:
            self._closing = False
