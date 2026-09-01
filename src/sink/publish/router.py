"""Fleet streaming runtime: bounded sample queue and the stream router.

The tap side (SampleQueue.put) is called from source callback threads and
must never block or raise; the router thread drains, rate-caps, batches,
spools and publishes. Spec §2/§4.
"""

import queue as queue_mod
from collections.abc import Callable

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
