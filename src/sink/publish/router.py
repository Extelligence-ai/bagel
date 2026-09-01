"""Fleet streaming runtime: bounded sample queue and the stream router.

The tap side (SampleQueue.put) is called from source callback threads and
must never block or raise; the router thread drains, rate-caps, batches,
spools and publishes. Spec §2/§4.
"""

import queue as queue_mod
from collections.abc import Callable

from src.sink.publish.config import ResolvedChannel

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
        """Pop all pending slot-winners, ordered by t, into a batch (or None)."""
        self._last_flush = t_batch
        if not self._pending:
            return None
        items = sorted(
            ((chan_name, slot, t, v) for (chan_name, slot), (t, v) in self._pending.items()),
            key=lambda item: item[2],
        )
        for chan_name, slot, _t, _v in items:
            prev = self._last_emitted_slot.get(chan_name)
            if prev is None or slot > prev:
                self._last_emitted_slot[chan_name] = slot
        self._pending.clear()
        samples = [{"c": chan_name, "t": t, "v": v} for chan_name, _slot, t, v in items]
        return {"v": 1, "t_batch": t_batch, "samples": samples}

    def should_flush(self, now: float, pending_count: int) -> bool:
        """Return True if the batch is big enough or the flush interval has elapsed."""
        return pending_count >= self.max_samples or now - self._last_flush >= self.flush_interval_s

    @property
    def pending_count(self) -> int:
        """Number of stored slot-winners awaiting the next flush."""
        return len(self._pending)
