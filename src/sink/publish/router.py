"""Fleet streaming runtime: bounded sample queue and the stream router.

The tap side (SampleQueue.put) is called from source callback threads and
must never block or raise; the router thread drains, rate-caps, batches,
spools and publishes. Spec §2/§4.
"""

import logging
import queue as queue_mod
import random
import threading
import time
from collections.abc import Callable

from src.sink.publish.config import ResolvedChannel
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.spool import Spool

Sample = tuple[str, float, dict]


class SampleQueue:
    """Bounded drop-oldest queue between the buffer tap and the router."""

    def __init__(self, maxsize: int) -> None:
        """Initialize a SampleQueue with the given maximum depth."""
        self._q: queue_mod.Queue[Sample] = queue_mod.Queue(maxsize=maxsize)
        self.dropped = 0

    def put(self, sample: Sample) -> None:
        """Enqueue a sample, non-blocking. On Full, drop the oldest and count it."""
        try:
            self._q.put_nowait(sample)
        except queue_mod.Full:
            try:
                self._q.get_nowait()
                self.dropped += 1
            except queue_mod.Empty:
                pass
            try:
                self._q.put_nowait(sample)
            except queue_mod.Full:
                self.dropped += 1

    def drain(self, max_items: int, timeout_s: float) -> list[Sample]:
        """Block up to timeout_s for the first item, then greedily drain up to max_items."""
        out: list[Sample] = []
        try:
            out.append(self._q.get(timeout=timeout_s))
        except queue_mod.Empty:
            return out
        while len(out) < max_items:
            try:
                out.append(self._q.get_nowait())
            except queue_mod.Empty:
                break
        return out

    @property
    def depth(self) -> int:
        """Current number of samples waiting in the queue."""
        return self._q.qsize()

    def as_tap(self) -> Callable[[str, float, dict], None]:
        """Return a bound callback suitable for TopicBufferWriter.set_tap()."""

        def tap(topic: str, t: float, msg: dict) -> None:
            self.put((topic, t, msg))

        return tap


def extract_value(msg: dict, path: list[str]) -> object:
    """Walk nested dict `msg` by `path`. Raises KeyError passthrough on missing."""
    value: object = msg
    for key in path:
        value = value[key]  # type: ignore[index]  # path walks nested dicts by contract
    return value


def _extract_channel_value(chan: ResolvedChannel, msg: dict) -> object:
    """Extract a channel's sample value from `msg` per its resolved paths.

    A scalar/bool channel has a single "value" path. A geo channel has "lat"
    and "lon" paths (required) and an optional "alt" path -- the "alt" key is
    included in the returned dict only when that path was resolved for this
    channel.
    """
    paths = chan.paths
    if "value" in paths:
        return extract_value(msg, paths["value"].path)
    geo: dict[str, object] = {
        "lat": extract_value(msg, paths["lat"].path),
        "lon": extract_value(msg, paths["lon"].path),
    }
    if "alt" in paths:
        geo["alt"] = extract_value(msg, paths["alt"].path)
    return geo


class RouterCore:
    """Pure extraction, rate-capping, and batch-building logic.

    No I/O, no spool, no seq -- the router thread (Task 3) drives offer()/
    flush()/should_flush() and owns sequencing and publishing.

    Slot-survival rule: `offer()` computes a per-channel slot index
    `int(t * rate_hz)` and keeps the later-`t` sample as that slot's winner
    (`_pending`, keyed by `(channel_name, slot)`). Once `flush()` pops a
    slot's winner into a batch, that slot is retired into
    `_last_emitted_slot[channel_name]` (a per-channel high-water mark). Any
    later `offer()` whose slot is <= that high-water mark is dropped instead
    of being stored -- otherwise a reordered/late sample could resurrect and
    re-emit a slot a subscriber already received in an earlier batch. This is
    simpler than re-deriving "already emitted" from history at flush time
    and keeps the check on the hot `offer()` path a single dict lookup.
    """

    def __init__(
        self,
        resolved: list[ResolvedChannel],
        flush_interval_s: float,
        max_samples: int = 500,
    ) -> None:
        """Index `resolved` channels by source topic and set batching limits."""
        self._channels_by_topic: dict[str, list[ResolvedChannel]] = {}
        for chan in resolved:
            self._channels_by_topic.setdefault(chan.source_topic, []).append(chan)
        self.flush_interval_s = flush_interval_s
        self.max_samples = max_samples
        self.skipped = 0
        self._pending: dict[tuple[str, int], tuple[float, object]] = {}
        self._last_emitted_slot: dict[str, int] = {}
        self._last_flush = 0.0

    def offer(self, topic: str, t: float, msg: dict) -> None:
        """Extract+rate-cap `msg` into each channel sourced from `topic`."""
        for chan in self._channels_by_topic.get(topic, []):
            try:
                value = _extract_channel_value(chan, msg)
            except (KeyError, TypeError):
                self.skipped += 1
                continue
            slot = int(t * chan.rate_hz)
            last_emitted = self._last_emitted_slot.get(chan.name)
            if last_emitted is not None and slot <= last_emitted:
                continue  # stale sample for a slot already flushed out
            key = (chan.name, slot)
            existing = self._pending.get(key)
            if existing is None or t >= existing[0]:
                self._pending[key] = (t, value)

    def flush(self, t_batch: float) -> dict | None:
        """Pop up to `max_samples` pending slot-winners, ordered by t, into a batch.

        Returns `None` if nothing is pending. `max_samples` caps this call's
        OWN output, not just a trigger threshold on `should_flush` (Codex
        review: it used to empty the ENTIRE `_pending` dict into one batch
        regardless of size, so a wide manifest with many channels
        distinct-slot-winning in the same interval could produce an
        arbitrarily large batch). If more than `max_samples` slot-winners are
        pending, the remainder stays in `_pending`, in the same t-order, for
        a subsequent `flush()` call to pick up -- the caller (`_tick`) loops
        calling this until it returns `None`, giving each chunk its own seq
        and spool append.
        """
        self._last_flush = t_batch
        if not self._pending:
            return None
        items = sorted(
            ((chan_name, slot, t, v) for (chan_name, slot), (t, v) in self._pending.items()),
            key=lambda item: item[2],
        )
        chunk = items[: self.max_samples]
        for chan_name, slot, _t, _v in chunk:
            prev = self._last_emitted_slot.get(chan_name)
            if prev is None or slot > prev:
                self._last_emitted_slot[chan_name] = slot
            del self._pending[(chan_name, slot)]
        samples = [{"c": chan_name, "t": t, "v": v} for chan_name, _slot, t, v in chunk]
        return {"v": 1, "t_batch": t_batch, "samples": samples}

    def should_flush(self, now: float, pending_count: int) -> bool:
        """Return True if the batch is big enough or the flush interval has elapsed."""
        return pending_count >= self.max_samples or now - self._last_flush >= self.flush_interval_s

    @property
    def pending_count(self) -> int:
        """Number of stored slot-winners awaiting the next flush."""
        return len(self._pending)


class StreamRouter(threading.Thread):
    """Drains the sample queue into RouterCore, spools batches, and publishes.

    Replay-then-live is emergent, not a separate code path: `_pump` always
    starts from `spool.pending("channels")`, and a freshly-flushed live
    batch is appended to the spool (with its seq) before `_pump` runs in the
    same tick. So a batch built from this tick's live samples is published
    only after every older, still-unacked spooled batch -- the spool is the
    single source of publish order, whether its records arrived from a prior
    offline period or from this tick.
    """

    INITIAL_BACKOFF_S = 1.0
    MAX_BACKOFF_S = 60.0

    def __init__(
        self,
        core: RouterCore,
        queue: SampleQueue,
        spool: Spool,
        publisher: Publisher,
        schema_payload: dict,
    ) -> None:
        """Wire the core, queue, spool and publisher together; does not start the thread."""
        super().__init__(daemon=True)
        self._core = core
        self._queue = queue
        self._spool = spool
        self._publisher = publisher
        self._schema_payload = schema_payload
        # Named _stop_event (not _stop) because threading.Thread already owns
        # a private _stop() method; shadowing it breaks Thread's own join().
        self._stop_event = threading.Event()
        self._online = False
        self._backoff = self.INITIAL_BACKOFF_S
        self._next_attempt = 0.0
        self._fatal_error: str | None = None

    def run(self) -> None:
        """Thin loop: tick until stop() is called. All logic lives in `_tick`.

        A `_tick` failure that is not a `PublishError` (already handled inside
        `_pump`) means a bug in `RouterCore` or `Spool`, not an offline
        broker. Rather than dying silently -- indistinguishable from a router
        that is merely still offline -- log it, record it as `_fatal_error`,
        and exit the loop so `alive`/`last_error` surface it to
        `FleetService.status()`.

        On a clean exit (the stop signal, not a fatal `_tick` error), this
        calls `_final_flush()` as the very last thing before returning --
        see its docstring for why samples the tap already accepted (via
        `offer()`) but that hadn't yet crossed a flush boundary would
        otherwise be silently dropped on `stop()` (Codex review).
        """
        while not self._stop_event.is_set():
            try:
                self._tick(time.time())
            except Exception as exc:
                logging.getLogger(__name__).exception("StreamRouter thread died")
                self._fatal_error = repr(exc)
                return
        self._final_flush()

    def _final_flush(self) -> None:
        """Drain the queue into the core and spool whatever it hands back, once.

        Runs as the last act of `run()`, after the stop signal has been
        observed and the tick loop has exited -- entirely on this thread, so
        it never races a concurrent `_tick()` touching the same
        `_core`/`_queue`/`_spool` (Codex review: `stop()` doing this
        directly on the CALLER's thread instead would race the router
        thread's own in-flight `_tick`). Mirrors `_tick`'s own
        drain-then-offer-then-flush-then-spool path, minus `_pump` --
        publishing the result is not this method's job, only making sure it
        is durably spooled for the router's normal replay-on-restart path
        (or the next `_pump` after a `resume()`) to pick up.

        `pause(discard=True)` is unaffected: it flushes here same as any
        other stop, then FleetService acks past the newly-spooled batch's
        seq, same as it always could for anything already in the spool.
        """
        while True:
            samples = self._queue.drain(max_items=500, timeout_s=0.0)
            if not samples:
                break
            for topic, t, msg in samples:
                self._core.offer(topic, t, msg)
        now = time.time()
        while (batch := self._core.flush(now)) is not None:
            seq = self._spool.next_seq("channels")
            batch["seq"] = seq
            self._spool.append("channels", seq, batch)

    def _tick(self, now: float) -> None:
        """One iteration: drain+offer+maybe-flush-to-spool, then publish.

        Unit tests drive this directly with controlled `now` values instead
        of sleeping; `queue.drain`'s bounded timeout is what paces the real
        thread loop in `run()`.

        `core.flush(now)` caps its own output at `max_samples` (Codex
        review), so a should_flush-triggered flush loops here until it
        returns `None` -- every chunk gets its own seq and its own spool
        append, so a wide manifest's oversized batch is fully drained and
        durably spooled within this one tick rather than trickling out over
        several.
        """
        timeout_s = min(0.2, self._core.flush_interval_s)
        samples = self._queue.drain(max_items=500, timeout_s=timeout_s)
        for topic, t, msg in samples:
            self._core.offer(topic, t, msg)
        if self._core.should_flush(now, self._core.pending_count):
            while (batch := self._core.flush(now)) is not None:
                seq = self._spool.next_seq("channels")
                batch["seq"] = seq
                self._spool.append("channels", seq, batch)
        self._pump(now)

    def _pump(self, now: float) -> None:
        """Publish the spool's backlog in seq order; go offline on the first failure.

        Always starts from `spool.pending("channels")` -- see class
        docstring for why that alone makes replay-then-live emergent.

        Checks `self._stop_event` between iterations: a post-outage backlog
        (the channels lane is capped but can still hold thousands of
        records) can take longer to drain than stop()'s join(timeout=5)
        bound. Breaking early leaves the remaining records spooled and
        unacked, which is correct -- they replay on the next start, in the
        same seq order, exactly like any other still-unacked backlog.

        Also checks `self._stop_event` first thing: without this, a
        `stop()` landing while `_reconnect()`'s blocking `connect()` call is
        in flight could still let this call fall through into a full
        publish pass on a connection that only just came up, past
        `join(5)`'s deadline -- this closes that stop/reconnect race at its
        narrowest point.
        """
        if self._stop_event.is_set():
            return
        if not self._online:
            if now < self._next_attempt:
                return
            self._reconnect(now)
            if not self._online:
                return
        for seq, payload in self._spool.pending("channels"):
            if self._stop_event.is_set():
                return
            try:
                self._publisher.publish_channels(payload)
            except PublishError:
                self._online = False
                self._schedule_retry(now)
                return
            self._spool.ack("channels", seq)

    def _reconnect(self, now: float) -> None:
        """Attempt one (re)connect + schema republish; stay offline on failure."""
        try:
            self._publisher.connect()
            self._publisher.publish_schema(self._schema_payload)
        except Exception:  # connect()/publish_schema() may raise broadly (transport, TLS, ...)
            self._schedule_retry(now)
            return
        self._backoff = self.INITIAL_BACKOFF_S
        self._online = True

    def _schedule_retry(self, now: float) -> None:
        """Double (capped) the backoff, then pick the next attempt with full jitter."""
        self._backoff = min(self.MAX_BACKOFF_S, self._backoff * 2)
        self._next_attempt = now + random.uniform(0, self._backoff)  # noqa: S311 -- jitter, not crypto

    def stop(self) -> None:
        """Signal the loop to stop and join with a bounded timeout.

        The join timeout (12s) is deliberately longer than the 10s
        `wait_for_publish` bound inside `MqttPublisher.publish` (Codex
        review): a 5s join could return while `_pump` was still blocked
        inside a QoS-1 publish call, so termination wasn't actually
        guaranteed before a caller (e.g. `pause()`/`resume()`) proceeded. If
        the thread is somehow still alive after this longer join, that is
        logged rather than silently ignored.

        `_online` is reset to False here too (Codex review): it used to
        survive a clean stop unchanged, so `FleetService.status()` kept
        reporting `online: true` for a router that had already stopped.
        """
        self._stop_event.set()
        self.join(timeout=12.0)
        if self.is_alive():
            logging.getLogger(__name__).warning("router thread did not terminate")
        self._online = False

    @property
    def online(self) -> bool:
        """Whether the router currently believes it has a live publisher session."""
        return self._online

    @property
    def backoff(self) -> float:
        """Current backoff ceiling (seconds) used for the next reconnect's full jitter."""
        return self._backoff

    @property
    def alive(self) -> bool:
        """Whether the thread is running AND has not died on an unhandled `_tick` error.

        Distinct from `online`: `online` tracks the broker session (false
        while offline-but-retrying, which is normal); `alive` tracks whether
        the router thread itself is still doing its job at all.
        """
        return self.is_alive() and self._fatal_error is None

    @property
    def last_error(self) -> str | None:
        """The `repr()` of the exception that killed the thread, if any."""
        return self._fatal_error
