"""FleetService: composes the tap/queue/router/heartbeat pieces into one lifecycle.

Spec §2/§5. `start()` resolves the manifest's `streams:` channels against the
sink's already-subscribed topic buffers, wires a tap on each referenced
buffer into a fresh `SampleQueue`, and starts the `StreamRouter` and
`HeartbeatThread` threads that drain it. `stop()`/`pause()`/`resume()` manage
that pair of daemon threads; `status()` returns the plain-JSON shape step 7's
`describe_stream_status` tool hands back directly.

This module (with `router.StreamRouter` before it) sets this repo's
thread-lifecycle precedent: every long-running fleet thread is
`daemon=True`, stops via a `threading.Event` (so `stop()`/`join()` never
requires a sleep-based poll and returns as soon as the event is observed),
and its `stop()` joins with a bounded timeout (5s) so a slow or stuck thread
can never hang the caller -- mirrored from `TopicSink.close()`
(`src/sink/base.py:323`)'s try/finally shape: `stop()`/`pause()` do their
teardown in a `try` and always release the publisher session in a `finally`,
so a failure partway through still leaves the service in a consistent,
re-enterable state.

Startup/manifest wiring (deciding *when* to call `start()`) is explicitly
out of scope here -- step 6 ruling, since it's conditioned on identity
existing, which step 6 delivers. `startup.py` is not touched by this module.
"""

import dataclasses
import time

from settings import settings
from src.sink.publish import require_fleet
from src.sink.publish.config import StreamsConfig
from src.sink.publish.heartbeat import HeartbeatThread, build_heartbeat, disk_free
from src.sink.publish.identity import Identity, renew, should_attempt_renewal
from src.sink.publish.publisher import Publisher
from src.sink.publish.router import RouterCore, SampleQueue, StreamRouter
from src.sink.publish.spool import Spool


class FleetService:
    """Owns one robot's fleet-streaming runtime: tap wiring + router + heartbeat."""

    def __init__(
        self,
        *,
        sink: object,
        streams: StreamsConfig,
        publisher: Publisher,
        spool: Spool,
        identity: Identity | None = None,
    ) -> None:
        """Store the collaborators; does not touch the sink, publisher, or spool yet.

        ``identity``, when given, is this robot's enrolled fleet identity
        (spec §6). It is optional -- most tests and a dev-insecure,
        unenrolled robot construct this service with none -- but when
        present it does two things: `status()`/the heartbeat payload carry
        `cert_expires_at`, and a certificate-renewal closure
        (`should_attempt_renewal`/`renew`) is wired into the
        `HeartbeatThread`'s `renewal_check` hook so renewal is attempted
        automatically, once per heartbeat tick, when it's due. See
        `_renewal_check`.
        """
        self._sink = sink
        self._streams = streams
        self._publisher = publisher
        self._spool = spool
        self._identity = identity
        # Seeded from identity.yaml's on-disk `last_renewal_attempt_at` (via
        # `load_identity`), not always `None`: this service's own lifetime is
        # the process's lifetime (see module docstring, ruling B), so this
        # field is never itself persisted again mid-run -- but the ON-DISK
        # value it started from must still be honored on a fresh process, or
        # a crashlooping robot inside the 30-day renewal window would fire a
        # fresh renewal attempt on every restart, ignoring the daily rate
        # limit (`_RENEWAL_MIN_INTERVAL_S`) entirely.
        self._last_renewal_attempt_at: float | None = (
            identity.last_renewal_attempt_at if identity is not None else None
        )

        self._resolved: list = []
        self._schema_payload: dict = {"v": 1, "channels": []}
        self._writers: dict[str, object] = {}  # topic -> writer, for taps

        self._queue: SampleQueue | None = None
        self._core: RouterCore | None = None
        self._router: StreamRouter | None = None
        self._heartbeat: HeartbeatThread | None = None
        self._started_at: float | None = None

        self._started = False
        self._paused = False

    # -- accessors ---------------------------------------------------------------

    @property
    def sink(self) -> object:
        """The `TopicSink` this service taps.

        Lets a caller (e.g. `startup.py`'s close hook) identity-compare a
        sink against the one this service is running against, without
        reaching into the private `_sink` attribute.
        """
        return self._sink

    @property
    def streams(self) -> StreamsConfig:
        """The `StreamsConfig` this service was constructed with."""
        return self._streams

    @property
    def channels(self) -> list[dict]:
        """A copy of the resolved schema payload's `channels` list.

        A copy, not a live reference: mutating the returned list must never
        affect this service's own schema payload.
        """
        return list(self._schema_payload["channels"])

    @property
    def paused(self) -> bool:
        """Whether this service is started AND currently paused.

        `self._started and self._paused` rather than `self._paused` alone: a
        fresh, never-started service also has `_paused = False`, so that bit
        alone can't distinguish "never started" from "resumed" -- both would
        read `False` either way here, but requiring `_started` too keeps this
        property's contract explicit for `control.fleet_status()`, which
        derives its `"running"` vs `"paused"` service state from this.
        """
        return self._started and self._paused

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        """Resolve channels against the sink's subscribed buffers, then run.

        Raises:
            RuntimeError: `start()` called while already started -- a
                double start is a programming error, not something to
                silently ignore or silently restart.
            FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.
            StreamConfigError: a configured `channels[].topic` is not
                currently subscribed on the sink (reused from
                `StreamsConfig.resolve`, which raises exactly this when a
                topic is missing from the `structs` mapping -- a topic not
                subscribed on the sink is simply absent from it here).

        """
        if self._started:
            raise RuntimeError("FleetService already started")
        require_fleet()
        configured_topics = {rule.topic for rule in self._streams.channels}
        structs = {
            topic: self._sink.buffer_writer(topic).struct
            for topic in configured_topics
            if topic in self._sink.subscribed_topics
        }
        resolved = self._streams.resolve(structs)

        self._resolved = resolved
        self._schema_payload = {
            "v": 1,
            "channels": [
                {
                    "c": channel.name,
                    "type": channel.type,
                    "unit": channel.unit,
                    "source_topic": channel.source_topic,
                    "source_field": channel.source_field,
                }
                for channel in resolved
            ],
        }
        self._writers = {
            topic: self._sink.buffer_writer(topic)
            for topic in sorted({channel.source_topic for channel in resolved})
        }

        self._launch_runtime()
        self._started = True
        self._paused = False

    def stop(self) -> None:
        """Idempotent: clear taps, stop heartbeat, stop router, then release the publisher.

        Heartbeat stops BEFORE `publisher.close()` so the clean-stop
        heartbeat that `close()` itself publishes (per step 3's
        `MqttPublisher.close`) is the last one anyone sees -- a still-running
        heartbeat thread ticking after that could otherwise republish
        `online: true` right behind it.
        """
        if not self._started:
            return
        try:
            self._clear_taps()
            if self._heartbeat is not None:
                self._heartbeat.stop()
            if self._router is not None:
                self._router.stop()
        finally:
            self._publisher.close()
            self._started = False
            self._paused = False

    def pause(self, discard: bool = False) -> None:
        """Go offline, keeping the resolved config so `resume()` needs no re-resolve.

        Idempotent: a no-op if not started or already paused. `discard=True`
        additionally acks the channels lane up to its last written seq,
        dropping any still-unacked backlog (events/heartbeat are never-drop
        lanes and are left untouched). Closes the publisher with
        `reason="paused"` (spec §3), so the retained clean-stop heartbeat a
        broker-side subscriber sees reads distinctly from a genuine `stop()`.
        """
        if not self._started or self._paused:
            return
        self._clear_taps()
        if self._heartbeat is not None:
            self._heartbeat.stop()
        if self._router is not None:
            self._router.stop()
        self._publisher.close(reason="paused")
        if discard:
            last_seq = self._spool.next_seq("channels") - 1
            self._spool.ack("channels", last_seq)
        self._paused = True

    def resume(self) -> None:
        """Restart threads (fresh queue/core/router/heartbeat) and reconnect.

        Idempotent: a no-op if not started or not paused. Router/heartbeat
        threads cannot be restarted once stopped (`threading.Thread` runs
        once), so this builds fresh instances rather than reusing the paused
        ones; the resolved config and writer set from `start()` carry over
        unchanged.
        """
        if not self._started or not self._paused:
            return
        self._launch_runtime()
        self._paused = False

    def status(self) -> dict:
        """Plain JSON-able snapshot: connection, counters, and per-lane spool stats."""
        spool_stats = self._spool.stats()
        return {
            "online": self._router.online if self._router is not None else False,
            "backoff": self._router.backoff if self._router is not None else None,
            "queue": {
                "depth": self._queue.depth if self._queue is not None else 0,
                "dropped": self._queue.dropped if self._queue is not None else 0,
            },
            "skipped": self._core.skipped if self._core is not None else 0,
            "spool": {lane: dataclasses.asdict(stats) for lane, stats in spool_stats.items()},
            "reconnects": getattr(self._publisher, "reconnects", 0),
            "subscriptions": self._subscriptions(),
            "channels_active": len(self._resolved),
            "router_alive": self._router.alive if self._router is not None else False,
            "router_error": self._router.last_error if self._router is not None else None,
            "heartbeat_spool_failures": (
                self._heartbeat.spool_failures if self._heartbeat is not None else 0
            ),
            "heartbeat_alive": self._heartbeat.alive if self._heartbeat is not None else False,
            "heartbeat_error": self._heartbeat.last_error if self._heartbeat is not None else None,
            "cert_expires_at": self._identity.expires_at if self._identity is not None else None,
        }

    # -- internals ---------------------------------------------------------------

    def _launch_runtime(self) -> None:
        """Build fresh queue/core/router/heartbeat, wire taps, and start both threads."""
        self._queue = SampleQueue(settings.FLEET_QUEUE_MAX_SAMPLES)
        self._core = RouterCore(self._resolved, self._streams.flush_interval_s)
        self._router = StreamRouter(
            self._core, self._queue, self._spool, self._publisher, self._schema_payload
        )
        tap = self._queue.as_tap()
        for writer in self._writers.values():
            writer.set_tap(tap)
        self._started_at = time.time()
        self._heartbeat = HeartbeatThread(
            self._publisher,
            self._heartbeat_payload,
            spool=self._spool,
            renewal_check=self._renewal_check if self._identity is not None else None,
        )
        self._router.start()
        self._heartbeat.start()

    def _clear_taps(self) -> None:
        for writer in self._writers.values():
            writer.set_tap(None)

    def _subscriptions(self) -> list[str]:
        return sorted(self._writers)

    def _heartbeat_payload(self) -> dict:
        return build_heartbeat(
            started_at=self._started_at if self._started_at is not None else time.time(),
            subscriptions=self._subscriptions(),
            channels_active=len(self._resolved),
            queue_depth=self._queue.depth if self._queue is not None else 0,
            queue_dropped=self._queue.dropped if self._queue is not None else 0,
            spool_stats=self._spool.stats(),
            disk_free_bytes=disk_free(settings.CACHE_DIRECTORY),
            reconnects=getattr(self._publisher, "reconnects", 0),
            cert_expires_at=self._identity.expires_at if self._identity is not None else None,
        )

    def _renewal_check(self) -> None:
        """Renew `self._identity`'s certificate if it's due (wired as the heartbeat's hook).

        Only ever installed on the `HeartbeatThread` when this service was
        constructed with an `identity` (see `_launch_runtime`), so
        `self._identity` is never `None` here. On a successful renewal,
        `self._identity` is swapped to the new `Identity` `renew()` returns
        -- but the LIVE publisher connection keeps authenticating with the
        OLD cert until its own next reconnect; this does not force one (see
        `identity.renew`'s docstring).

        Exceptions are deliberately NOT caught here: `HeartbeatThread._tick`
        already runs `renewal_check` inside its own try/except, and `renew()`
        itself never raises -- so there is nothing left for this method to
        guard against beyond what those two already cover.

        NOTE: `renew()` does a real mTLS POST with a 30s `urlopen` timeout,
        and this runs synchronously on the heartbeat thread (it IS the
        thread's `renewal_check` hook) -- so a slow/hanging renewal endpoint
        can delay one heartbeat tick by up to a full interval. This happens
        at most ~once/day (`_RENEWAL_MIN_INTERVAL_S`) and only in the last 30
        days before cert expiry; accepted trade-off, not a bug.
        """
        if self._identity is None:  # pragma: no cover -- guarded by _launch_runtime's wiring
            return
        now = time.time()
        due = should_attempt_renewal(now, self._identity.expires_at, self._last_renewal_attempt_at)
        if not due:
            return
        self._last_renewal_attempt_at = now
        new_identity = renew(self._identity)
        if new_identity is not None:
            # CRITICAL: point the LIVE publisher at the new cert/key BEFORE
            # (or atomically with) swapping self._identity -- renew()'s
            # pointer-commit already unlinked the old files, so the very
            # next reconnect must not still be holding their paths (see
            # MqttPublisher.set_tls's docstring). Not every Publisher
            # implementation has this seam (e.g. tests' FakePublisher), so
            # it's called only when present.
            set_tls = getattr(self._publisher, "set_tls", None)
            if set_tls is not None:
                set_tls(
                    tls_ca_certs=str(new_identity.ca_path),
                    tls_certfile=str(new_identity.cert_path),
                    tls_keyfile=str(new_identity.key_path),
                )
            self._identity = new_identity
