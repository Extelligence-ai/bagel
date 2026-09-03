"""Store-and-forward disk spool for fleet streaming (spec §4).

Per-lane append-only JSONL segments named by their first seq
(``segment-{first_seq:016d}.jsonl``), a per-lane acked-seq watermark in
``watermark.json`` (atomic replace), delete-only-on-ack, drop-oldest
eviction for capped lanes, never-drop for the rest.

Durability posture matches the repo: the watermark is rename-atomic;
segment appends are plain writes (no fsync), so a crash may lose the
active segment's tail — QoS-1 at-least-once plus cloud-side seq dedupe
absorbs the replay. Concurrency: every mutator holds the spool's file
lock (one lock per spool root, like the sink buffer's per-topic locks).
`exclusive()` lets a caller hold that same lock across several mutating
calls as one atomic unit (e.g. the selftest CLI, Codex round 3 P1b).

Allocate-then-append MUST go through `append_next()`, never a separate
`next_seq()` call followed by `append()` (Codex round 3 P1, comment
3924082774): those are two separate lock acquisitions, so a competing
writer's `exclusive()` (or its own `next_seq()`/`append()` pair) can run
in the gap between them, and the disk-authoritative floor added earlier
this round then makes the SECOND caller's `append()` raise `ValueError`
against a seq it allocated against a now-stale floor -- exactly the
router's `_tick`/`_final_flush` batch-spool path, which had that
`ValueError` propagate uncaught out of `_tick()` and kill the whole
`StreamRouter` thread (see `router.py`, and its own docstring update).
`append_next()` derives the floor, allocates, and writes in ONE critical
section, so no caller can ever be interleaved between allocation and
write. `next_seq()` remains for genuine read-only introspection only
(e.g. heartbeat prune-window math) -- see its docstring.
"""

import dataclasses
import json
import logging
import os
import pathlib
import tempfile
from collections.abc import Callable, Iterator

import filelock

SEGMENT_MAX_BYTES = 4 * 1024 * 1024
NEVER_CAPPED_LANES = ("heartbeat",)


class SpoolError(Exception):
    """Raised when the spool cannot honor a request."""


class SpoolFullError(SpoolError):
    """Raised when a never-drop lane cannot write (disk full or failing)."""


class SpoolCorruptError(SpoolError):
    """Raised when a segment has mid-file JSON corruption (not a crash-torn final line)."""


class SpoolLockedError(SpoolError):
    """Raised when `Spool.exclusive()` cannot acquire the spool's lock within its timeout.

    Means a DIFFERENT `Spool` instance (almost always a different process --
    e.g. a live `FleetService`) is currently mid-operation on this same
    spool root. Never raised by the ordinary per-call mutators
    (`next_seq`/`append`/`ack`/`stats`) themselves -- those block
    indefinitely on the lock, matching the single-writer invariant's
    existing behavior. Only `exclusive()`'s bounded wait can time out.
    """


@dataclasses.dataclass
class LaneStats:
    """Per-lane counters for heartbeat/status reporting."""

    bytes: int
    pending: int
    last_seq: int
    acked_seq: int
    evicted: int = 0  # Records discarded by eviction; exact if contiguous, may over-count if gapped


def _segment_name(first_seq: int) -> str:
    return f"segment-{first_seq:016d}.jsonl"


def _first_seq_of(path: pathlib.Path) -> int:
    return int(path.stem.split("-", 1)[1])


def _scan_segment(
    segment: pathlib.Path, *, lane: str, tolerate_torn_tail: bool = False
) -> tuple[list[dict], int, bool]:
    r"""Parse JSONL lines from a segment, tracking the good-data boundary.

    Reads raw bytes (not text) so the returned offset is directly usable with
    ``os.truncate``. A "line" is a chunk ending in ``b"\n"``; the final chunk of
    a file that does not end in a newline is a crash-torn write in progress.

    Args:
        segment: Path to segment file.
        lane: Lane name, used only for the corruption error message.
        tolerate_torn_tail: If True, a torn or unparseable *final* line is dropped
            instead of raised. Corruption in any earlier line always raises.

    Returns:
        (records, good_end_offset, torn):
            records: Parsed JSON objects, in file order, excluding a dropped torn tail.
            good_end_offset: Byte offset immediately after the last good line's
                trailing newline (0 if the segment has no good lines). Truncating
                the file to this offset discards exactly the torn tail, if any.
            torn: True if the final line was torn/unparseable and dropped.

    Raises:
        SpoolCorruptError: A non-final line fails to parse, or the final line fails
            to parse and tolerate_torn_tail is False.

    """
    data = segment.read_bytes()
    chunks = data.splitlines(keepends=True)
    records: list[dict] = []
    good_end = 0
    torn = False
    for i, chunk in enumerate(chunks):
        is_final = i == len(chunks) - 1
        line_no = i + 1
        if not chunk.endswith(b"\n"):
            if is_final and tolerate_torn_tail:
                torn = True
                break
            raise SpoolCorruptError(
                f"lane '{lane}': segment '{segment.name}': truncated line {line_no} (no newline)"
            )
        try:
            record = json.loads(chunk.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            if is_final and tolerate_torn_tail:
                torn = True
                break
            raise SpoolCorruptError(
                f"lane '{lane}': segment '{segment.name}': corrupt JSON at line {line_no}"
            ) from exc
        records.append(record)
        good_end += len(chunk)
    return records, good_end, torn


class Spool:
    """One robot's outbox: lanes of segments plus an acked watermark.

    Callers must maintain the single-writer invariant per lane: only one thread/process
    appends to a lane at a time. Concurrent producers will collide with ValueError.
    """

    def __init__(self, root: pathlib.Path, capped_lanes: dict[str, int] | None = None) -> None:
        """Initialize spool at root with optional per-lane size caps.

        Args:
            root: Path to spool root directory.
            capped_lanes: Optional mapping of lane names to byte caps.

        """
        self._root = pathlib.Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._capped = dict(capped_lanes or {})
        forbidden = set(self._capped) & set(NEVER_CAPPED_LANES)
        if forbidden:
            raise ValueError(
                f"lanes {sorted(forbidden)} are never-drop (spec §4) and cannot be byte-capped"
            )
        self._lock = filelock.FileLock(str(self._root / ".lock"))
        self._last_seq: dict[str, int] = {}
        self._evicted: dict[str, int] = {}

    # -- paths ---------------------------------------------------------------

    def _lane_dir(self, lane: str, *, create: bool = True) -> pathlib.Path:
        path = self._root / lane
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _segments(self, lane: str, *, create: bool = True) -> list[pathlib.Path]:
        """List a lane's segments in order.

        Read paths (pending/stats/ack) must pass create=False: a never-written
        lane has no directory, and a read must not conjure one into existence
        (that would pollute stats with a phantom, permanently-empty lane).
        """
        lane_dir = self._lane_dir(lane, create=create)
        if not lane_dir.exists():
            return []
        return sorted(lane_dir.glob("segment-*.jsonl"))

    # -- watermark (read side; persistence arrives with ack()) ----------------

    def _watermarks(self) -> dict[str, int]:
        path = self._root / "watermark.json"
        if not path.exists():
            return {}
        return {str(k): int(v) for k, v in json.loads(path.read_text()).items()}

    def _watermark(self, lane: str) -> int:
        return self._watermarks().get(lane, 0)

    # -- seq allocation --------------------------------------------------------

    def _scan_last_seq(self, lane: str) -> int:
        """Recover the last written seq for a lane and seal its active segment.

        Segment file names encode history: even when the last segment is empty or
        wholly torn by a crash, the previous segment's records prove seqs up to
        ``_first_seq_of(last_segment) - 1`` were already written, so the recovered
        seq must never fall below that floor (falling below it would let ``next_seq``
        reissue already-used seqs). If the last segment has a crash-torn tail, this
        also truncates the file to the last good line's end offset so the next
        ``append`` starts from a clean line boundary instead of writing onto the
        partial line.
        """
        segments = self._segments(lane)
        if not segments:
            return self._watermark(lane)
        last_segment = segments[-1]
        records, good_end, torn = _scan_segment(last_segment, lane=lane, tolerate_torn_tail=True)
        if torn and good_end < last_segment.stat().st_size:
            os.truncate(last_segment, good_end)
        last = records[-1]["seq"] if records else 0
        floor = _first_seq_of(last_segment) - 1
        return max(last, floor, self._watermark(lane))

    def _tail_last_seq(self, lane: str) -> tuple[int | None, bool]:
        r"""Peek the highest seq already on disk in O(tail), not O(whole active segment).

        Appends only ever land at the end of the active segment, so the
        highest already-written seq for the lane is always in that
        segment's LAST complete line -- there's no need to parse every
        earlier line the way `_scan_last_seq` does (that full scan exists
        for crash-tail recovery on a lane's first touch, not for a cheap
        per-call disk check). Reads backward from the end in a small number
        of bounded, growing chunks instead of the whole file.

        Safe to call while holding `self._lock`: no other `Spool` instance
        can be mid-write to this segment concurrently, so the only reason
        the tail could be untermianted/unparsable is a stale unclean-
        shutdown remnant. Unlike the tail-content parsing below, THIS
        function does not repair that itself -- it only reports it (see
        `terminated` below); `_current_last_seq` is what reseals.

        Terminatedness is checked explicitly (Codex round 3 follow-up, PR
        #214 P1 on comment 3923824688), not inferred from whether the tail
        parses: a write is `line = json.dumps(...) + "\n"`, one `write()`
        call, but a crash can still land the file's true end anywhere
        inside or after that write. Two prior failure modes both left a
        write, in "a" (append) mode, land directly onto an unterminated
        tail with no separating newline -- corrupting the file into one
        unparsable line, discovered only much later when `pending()` raises
        `SpoolCorruptError`:
          - The crash lands exactly after a COMPLETE JSON payload but
            before its trailing `\n`. The old code parsed that tail as a
            legitimately committed record (it parses fine without the
            newline) and reported its seq as trustworthy.
          - The crash lands mid-payload (a genuinely partial JSON tail).
            The old code's `json.loads` failed as expected, but nothing
            then TRUNCATED the partial bytes before the next `append()`
            wrote onto them.
        Checking the segment's LAST BYTE (`O(1)`, a single `seek(-1,
        SEEK_END)` + one-byte read) sidesteps both: if it isn't `b"\n"`,
        the tail is torn regardless of what it parses as, full stop --
        `_current_last_seq` reseals (truncates) before anything else
        happens, so this function doesn't even attempt to parse a tail it
        already knows is untrustworthy.

        The window grows GEOMETRICALLY (doubling each retry: 8KiB, 16KiB,
        32KiB, ...), not by a fixed 8KiB step (Codex round 3 follow-up, PR
        #214 P1): a fixed step re-reads the whole (growing) window every
        iteration, so a final record bigger than one chunk -- worst case a
        segment-max single-line record, ~4MB -- would cost ~512 iterations
        each re-reading up to 4MB: quadratic total bytes read. Doubling
        bounds total bytes read to O(the eventual window size), i.e. O(tail
        size), since a geometric series sums to about 2x its last term.

        Returns:
            `(seq, terminated)`. `terminated` is `True` iff the active
            segment is empty/absent, or its true last byte is `b"\n"` -- a
            properly durable record boundary. `seq` is the last complete
            line's `seq`, or `None` if there is no active segment, it's
            empty, the tail isn't terminated (in which case `seq` is always
            `None` -- see above, it's never even parsed), or it doesn't
            parse (the caller falls back to the watermark / full scan).

        """
        segments = self._segments(lane, create=False)
        if not segments:
            return None, True
        path = segments[-1]
        size = path.stat().st_size
        if size == 0:
            return None, True
        window = min(size, 8192)
        data = b""
        with open(path, "rb") as handle:
            handle.seek(-1, os.SEEK_END)
            if handle.read(1) != b"\n":
                return None, False
            while True:
                handle.seek(size - window)
                data = handle.read(window)
                if data.rstrip(b"\n").count(b"\n") >= 1 or window >= size:
                    break
                window = min(size, window * 2)
        last_line = data.rstrip(b"\n").rsplit(b"\n", 1)[-1]
        if not last_line:
            return None, True
        try:
            record = json.loads(last_line.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None, True
        seq = record.get("seq")
        return (seq if isinstance(seq, int) else None), True

    def _current_last_seq(self, lane: str) -> int:
        """Disk-authoritative last-written seq for `lane` -- cheap on the common path.

        Codex round 3 follow-up (PR #214, P1): `next_seq()`/`append()`
        previously trusted `self._last_seq[lane]` unconditionally once a
        lane had been touched once in this process's lifetime, checking
        monotonicity only against that in-process cache. `exclusive()`
        (P1b) serializes concurrent DISK ACCESS, but does nothing to refresh
        a DIFFERENT already-open `Spool` instance's stale cache -- a
        long-lived instance (e.g. a `FleetService`) whose cache predates an
        intervening writer on the same real spool (e.g. a selftest run)
        would accept/allocate a seq that collides with what's already on
        disk: a silent DUPLICATE write, not the clean `ValueError` the
        single-writer invariant is supposed to guarantee.

        First touch (lane not yet in `self._last_seq`) keeps the existing,
        crash-tail-tolerant `_scan_last_seq` path unchanged -- unavoidable
        and needed exactly once, to recover from (and truncate) a possible
        unclean-shutdown remnant. Every later call instead re-derives the
        floor from disk cheaply: the watermark plus `_tail_last_seq`'s
        tail-only peek, maxed against the cache purely as a floor (never a
        source of truth) so this can never regress below what THIS instance
        itself already wrote.

        Design choice (comment 3923824688): the O(1) unterminated-tail
        check is folded INTO `_tail_last_seq` (returning a `(seq,
        terminated)` pair) rather than duplicated as a separate check in
        `append()`'s write path. This is the single place both `next_seq()`
        and `append()` funnel through, so a warm cache gets resealed by
        either call, not just `append()` -- and there's exactly one piece
        of code that knows how to interpret "is this segment's tail
        trustworthy", not two copies that could drift apart. When
        `_tail_last_seq` reports `terminated=False` -- OR reports `seq is
        None` while a segment file DOES exist (empty or unparsable; Codex
        round 3 follow-up, PR #214 P1 on comment 3924387659 -- see below),
        this reseals via the same crash-tail-tolerant `_scan_last_seq` first
        touch already uses (it truncates any untrustworthy bytes -- correct:
        no trailing newline means the record was never durably committed --
        and recomputes the floor from what's left, with the watermark still
        guarding against ever regressing below an already-acked seq).

        Why a `None` tail on an EXISTING segment must reseal too, not just
        fall back to the watermark: a `None` tail means "no active-segment
        record was found" (the segment is empty -- e.g. a roll that created
        the file but crashed before writing its first record -- or its tail
        line doesn't parse). The watermark alone is NOT a safe floor there:
        the segment's own FILENAME encodes a floor (`_first_seq_of(segment)
        - 1`, exactly what `_scan_last_seq` derives), and EARLIER segments
        can hold live, still-unacked records with seqs above the watermark.
        Falling back to the watermark alone would ignore both, letting a
        warm cache allocate a seq that collides with data already on disk
        (a floor REGRESSION, not merely a stale-but-safe undercount) -- so
        this is folded into the very same reseal branch as an unterminated
        tail, not treated as a separate "safe to just use the watermark"
        case. Only the genuine "no segment file exists at all" case (a lane
        truly never touched) has no filename to derive a floor from --
        `_scan_last_seq` handles that identically anyway (`if not segments:
        return self._watermark(lane)`), so resealing unconditionally
        whenever `tail is None` is both correct and just as cheap in that
        case (a directory glob, no file I/O).

        The disk-derived floor on the normal (non-reseal) path is stored
        back into `self._last_seq[lane]` too (Codex round 3 follow-up, PR
        #214 P2 on comment 3924387659): previously it was computed fresh on
        every call but discarded instead of cached, so the cache could
        silently lag behind what this instance's OWN reads had already
        established on disk. Storing it is always safe -- it's folded
        through `max()` with the existing cache, so it can only advance,
        never regress, and it costs nothing extra to write.
        """
        if lane not in self._last_seq:
            self._last_seq[lane] = self._scan_last_seq(lane)
            return self._last_seq[lane]
        tail, terminated = self._tail_last_seq(lane)
        if not terminated or tail is None:
            self._last_seq[lane] = self._scan_last_seq(lane)
            return self._last_seq[lane]
        disk_floor = max(tail, self._watermark(lane))
        current = max(disk_floor, self._last_seq[lane])
        self._last_seq[lane] = current
        return current

    def exclusive(self, timeout: float = 5.0) -> filelock.AcquireReturnProxy:
        """Hold this spool's lock across an extended, multi-call critical section.

        For callers that need more than one mutating call (`next_seq`/
        `append`/`ack`/`stats`) to run as a single atomic unit against this
        spool root -- e.g. the selftest CLI, which must not interleave with a
        concurrently running `FleetService` writing the same real spool
        (Codex round 3, P1b). Use it as a context manager:

            with spool.exclusive(timeout=5.0):
                spool.next_seq(...)
                spool.append(...)
                spool.ack(...)

        Reentrant with those calls: `filelock.FileLock` (used here and by
        every mutator below) is thread-local reentrant, and this method
        acquires the SAME `FileLock` instance every mutator already uses --
        so calls made inside the `with` block just increment/decrement its
        counter instead of blocking on a lock this thread already holds.
        Only safe from the thread that entered the context; a lock held by a
        DIFFERENT `Spool` instance (in practice, almost always a different
        process) is a real OS-level file lock and is waited on normally.

        Args:
            timeout: Seconds to wait for a lock held elsewhere before giving
                up. Short and bounded on purpose: this is a refusal, not an
                indefinite wait -- unlike the per-call mutators, which block
                without a timeout.

        Returns:
            A context manager that releases the lock (or decrements its
            reentrant counter) on exit.

        Raises:
            SpoolLockedError: the lock is held elsewhere and was not
                released within `timeout` seconds.

        """
        try:
            return self._lock.acquire(timeout=timeout)
        except filelock.Timeout as exc:
            raise SpoolLockedError(
                f"spool at {self._root} is locked by another writer "
                f"(timed out after {timeout}s)"
            ) from exc

    def next_seq(self, lane: str) -> int:
        """Peek the next sequence number for a lane -- READ-ONLY introspection.

        NOT for allocate-then-append (Codex round 3 P1, comment 3924082774):
        this returns a snapshot that is immediately stale the instant the
        lock releases. A separate `next_seq()` call followed by a separate
        `append()` call is TWO lock acquisitions with a gap between them in
        which any other writer (another `Spool` instance's own
        `next_seq()`/`append()` pair, or a `spool.exclusive()`-held run)
        can advance the lane -- the disk-authoritative floor (this round)
        then makes the second caller's `append()` raise `ValueError`
        against a seq it allocated against a floor that's since moved. That
        used to just mean a clean, if surprising, `ValueError`; it turned
        out to also be exactly what could kill the live `StreamRouter`
        thread (see `router.py` and its docstring) when the interleaving
        landed on ITS `next_seq()`/`append()` pair. Every allocate-then-
        write caller must use `append_next()` instead, which does both
        under ONE lock acquisition. This method is for genuine read-only
        uses that never write anything off the result -- e.g. heartbeat
        prune-window math that only needs to know roughly where a lane is.

        Disk-authoritative (Codex round 3 follow-up, PR #214 P1): re-derives
        the floor from disk on every call after the lane's first touch --
        see `_current_last_seq` -- so a concurrent writer (a different
        `Spool` instance on the same root) that advanced the lane since this
        instance last looked is reflected here too.

        Args:
            lane: Lane name.

        Returns:
            Next monotonic sequence number (1-based), valid only as a
            snapshot at the moment this call returns.

        Raises:
            SpoolCorruptError: The lane's active segment has mid-file corruption
                (only on the first call for this lane in this process's lifetime).

        """
        with self._lock:
            return self._current_last_seq(lane) + 1

    # -- append ----------------------------------------------------------------

    def append(self, lane: str, seq: int, payload: dict) -> None:
        """Append a record to a lane at a CALLER-CHOSEN seq.

        For a caller that already knows the exact seq it must use (e.g.
        replaying/reconciling a specific record). For the far more common
        "give me the next seq and write my payload there" pattern, use
        `append_next()` instead -- it's the only way to do that atomically
        (see its docstring and `next_seq()`'s for why a separate `next_seq()`
        + `append()` pair is unsafe).

        Args:
            lane: Lane name.
            seq: Sequence number (must be > last written seq).
            payload: JSON-serializable record.

        Raises:
            ValueError: If seq is not monotonic against the DISK-authoritative
                last-written seq (Codex round 3 follow-up, PR #214 P1) --
                checked fresh on every call after the lane's first touch, not
                just against this instance's in-process cache, so a
                concurrent writer's advance on the same real spool root is
                always caught cleanly here rather than silently duplicated.
            SpoolFullError: If lane is never-capped and write fails.
            SpoolError: If lane is capped and write fails.
            SpoolCorruptError: The lane's active segment has mid-file corruption
                (only on the first call for this lane in this process's lifetime).

        """
        with self._lock:
            current = self._current_last_seq(lane)
            self._last_seq[lane] = current
            if seq <= current:
                raise ValueError(f"seq must be monotonic: got {seq}, last was {current}")
            self._write_locked(lane, seq, payload)

    def append_next(self, lane: str, build: Callable[[int], dict]) -> int:
        """Atomically allocate the next seq AND append it -- ONE critical section.

        The structural fix for the allocate-then-append race (Codex round 3
        P1, comment 3924082774): deriving the floor, assigning `seq =
        floor + 1`, and writing all happen under a SINGLE acquisition of
        `self._lock`. No other writer -- another `Spool` instance's own
        call, or a `spool.exclusive()`-held run -- can observe this lane's
        state, allocate a colliding seq, or write in between; by
        construction, there is no gap left for one to land in. This is now
        the required path for every allocate-then-write caller (the live
        router's batch-spool path, the selftest's channel/event appends) --
        `next_seq()` followed by a separate `append()` call is exactly the
        two-lock-acquisition pattern that made this race possible, and
        remains unsafe for anything that writes off the result.

        Args:
            lane: Lane name.
            build: Called with the allocated seq, INSIDE the lock, to
                produce the JSON-serializable payload -- e.g.
                ``lambda seq: {**batch, "seq": seq}`` or a small function
                that stamps `payload["seq"] = seq` on an already-built dict
                and returns it. This is the one point where the wire
                payload's own embedded `seq` field (distinct from the
                spool's own outer JSONL wrapper, which always carries `seq`
                regardless) gets set, so it's guaranteed to match the
                atomically-allocated value -- never a value observed,
                computed, or raced against before the lock was held.

        Returns:
            The allocated seq.

        Raises:
            SpoolFullError: If lane is never-capped and write fails.
            SpoolError: If lane is capped and write fails.
            SpoolCorruptError: The lane's active segment has mid-file corruption
                (only on the first call for this lane in this process's lifetime).

        """
        with self._lock:
            current = self._current_last_seq(lane)
            self._last_seq[lane] = current
            seq = current + 1
            payload = build(seq)
            self._write_locked(lane, seq, payload)
            return seq

    def _write_locked(self, lane: str, seq: int, payload: dict) -> None:
        """Write one record to `lane` at `seq` -- caller must already hold `self._lock`.

        Shared by `append()` (caller-chosen seq, already floor-checked) and
        `append_next()` (seq just allocated in the same critical section):
        both have already established that `seq` is the correct next value
        for this lane by the time this runs; this only does the actual
        line-write, oversized-drop, segment-roll, and eviction bookkeeping.
        """
        line = json.dumps({"seq": seq, "payload": payload}) + "\n"
        # Drop-oldest semantics extend to drop-oversized (Codex review):
        # _evict() only ever unlinks while more than one segment exists,
        # so a single record whose own serialized size alone exceeds
        # SEGMENT_MAX_BYTES becomes its own sole/newest segment and can
        # never be evicted -- unboundedly blowing past a capped lane's
        # byte cap. Refuse to write it instead: consume the seq (so the
        # caller's sequencing stays monotonic and no retry re-attempts
        # the same doomed record) and count it as evicted. EXCEPTION:
        # never drop on never-drop lanes (spec §4) -- their payloads are
        # tiny, so this is in practice unreachable, but the never-drop
        # ruling still applies if it somehow were.
        if lane not in NEVER_CAPPED_LANES and len(line.encode("utf-8")) > SEGMENT_MAX_BYTES:
            self._evicted[lane] = self._evicted.get(lane, 0) + 1
            logging.getLogger(__name__).warning(
                "lane '%s': dropping oversized record seq=%d (%d bytes > SEGMENT_MAX_BYTES=%d)",
                lane,
                seq,
                len(line.encode("utf-8")),
                SEGMENT_MAX_BYTES,
            )
            self._last_seq[lane] = seq
            return
        segments = self._segments(lane)
        active = segments[-1] if segments else None
        rolled = False
        if active is None or active.stat().st_size + len(line) > SEGMENT_MAX_BYTES:
            active = self._lane_dir(lane) / _segment_name(seq)
            rolled = True
        try:
            with open(active, "a", encoding="utf-8") as handle:
                handle.write(line)
        except OSError as exc:
            if lane not in self._capped:
                raise SpoolFullError(f"lane '{lane}': {exc}") from exc
            raise SpoolError(f"lane '{lane}': {exc}") from exc
        self._last_seq[lane] = seq
        if rolled:
            self._evict(lane)

    # -- ack & watermark --------------------------------------------------------

    def ack(self, lane: str, seq: int) -> None:
        """Advance the lane's watermark to ``seq`` and prune acked segments.

        Advance-to semantics: any ``seq`` above the watermark becomes the new
        watermark (skipped seqs are treated as acked — the publisher sends and
        acks in order); ``seq`` at or below it is an idempotent no-op on the
        watermark itself, but still retries ``_prune`` (see below).

        The idempotent (``seq`` at or below the watermark) path still calls
        ``_prune`` against the persisted watermark before returning: if a
        prior process died after ``_write_watermarks`` committed but before
        ``_prune`` ran, the watermark is already at (or beyond) ``seq`` while
        the segments it covers were never actually pruned — a repeated ack
        for that same seq is exactly the retry that must finish the job, or
        those segments are never pruned (Codex review).
        """
        with self._lock:
            marks = self._watermarks()
            if seq <= marks.get(lane, 0):
                self._prune(lane, marks.get(lane, 0))
                return
            marks[lane] = seq
            self._write_watermarks(marks)
            self._prune(lane, seq)

    def _write_watermarks(self, marks: dict[str, int]) -> None:
        target = self._root / "watermark.json"
        fd, tmp = tempfile.mkstemp(prefix=f".{target.name}.", dir=self._root)
        tmp_path = pathlib.Path(tmp)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(marks, handle)
            os.replace(tmp_path, target)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _prune(self, lane: str, watermark: int) -> None:
        segments = self._segments(lane, create=False)
        for index, segment in enumerate(segments):
            if index + 1 < len(segments):
                last_in_segment = _first_seq_of(segments[index + 1]) - 1
            else:
                break  # never prune the active (last) segment here
            if last_in_segment <= watermark:
                segment.unlink()

    def _evict(self, lane: str) -> None:
        """Unlink oldest segments when lane bytes exceed cap.

        Evicted-count tracking counts records above the watermark that were unlinked.
        For contiguous seqs, the count is exact; with seq gaps, it may over-count (erring
        toward reporting data loss). Seqs below the watermark are never counted as loss.
        """
        cap = self._capped.get(lane)
        if cap is None:
            return
        segments = self._segments(lane)
        total = sum(p.stat().st_size for p in segments)
        watermark = self._watermark(lane)
        while total > cap and len(segments) > 1:
            oldest = segments.pop(0)
            last_in_oldest = _first_seq_of(segments[0]) - 1
            if last_in_oldest > watermark:
                self._evicted[lane] = self._evicted.get(lane, 0) + (
                    last_in_oldest - max(watermark, _first_seq_of(oldest) - 1)
                )
            total -= oldest.stat().st_size
            oldest.unlink()

    def stats(self) -> dict[str, LaneStats]:
        """Get per-lane statistics (bytes, pending, seqs, evicted count).

        Disk-authoritative `last_seq` (Codex round 3 follow-up, PR #214 P2
        on comment 3925391258): previously `self._last_seq.get(lane) or
        self._scan_last_seq(lane)` trusted this instance's own cache
        WHENEVER it was already warm (non-zero) -- unlike `acked`
        (`_watermarks()`) and `pending` (`self.pending()`), both of which
        already re-read fresh from disk on every call. A warm cache left
        stale by another writer advancing the same real lane (e.g. a live
        `FleetService` whose heartbeat calls `stats()` while a selftest run
        on a separate instance appends past where this instance last
        looked) meant a heartbeat's reported `last_seq` -- and by extension
        an operator's read of how far behind that lane's backlog actually
        is -- silently lagged reality. Routed through `_current_last_seq`
        instead: the same disk-authoritative derivation `next_seq()`/
        `append()` use, cheap on the common path and storing the fresher
        value back through the cache as a never-regress floor.

        Returns:
            Dict mapping lane name to LaneStats.

        """
        with self._lock:
            marks = self._watermarks()
            out: dict[str, LaneStats] = {}
            # A lane whose every append so far was an oversized-and-dropped
            # record (see `append()`) never gets a directory -- union in
            # `self._evicted`'s lanes too, or that lane's evicted count would
            # be invisible here even though it's real (Codex review: the
            # counter must actually be observable, not just incremented).
            lanes = {p.name for p in self._root.iterdir() if p.is_dir()} | set(self._evicted)
            for lane in sorted(lanes):
                segments = self._segments(lane, create=False)
                acked = marks.get(lane, 0)
                last = self._current_last_seq(lane)
                pending = sum(1 for _ in self.pending(lane))
                out[lane] = LaneStats(
                    bytes=sum(p.stat().st_size for p in segments),
                    pending=pending,
                    last_seq=last,
                    acked_seq=acked,
                    evicted=self._evicted.get(lane, 0),
                )
            return out

    @classmethod
    def for_robot(cls, robot: str) -> "Spool":
        """Create a Spool for a robot with default caps.

        Args:
            robot: Robot identifier in shape tenant/robot or robot (one `/` max).
                   Must not contain `.`, `..`, or be empty. No leading `/`.

        Returns:
            Spool configured with default channels lane cap.

        Raises:
            ValueError: If robot contains path traversal or is malformed.

        """
        from settings import settings

        # Validate robot identifier to prevent path traversal
        if not robot:
            raise ValueError("robot must be non-empty")
        if robot.startswith("/"):
            raise ValueError("robot must not start with /")
        segments = robot.split("/")
        max_segments = 2  # tenant/robot or robot (0 vs 1 slash)
        if len(segments) > max_segments:
            raise ValueError("robot must have at most one / (format: robot or tenant/robot)")
        for segment in segments:
            if not segment or segment in (".", ".."):
                raise ValueError("robot segments must be non-empty and not . or ..")

        # Belt-and-braces: ensure resolved path is within publish directory
        base = (pathlib.Path(settings.CACHE_DIRECTORY) / "publish").resolve()
        root = (base / robot).resolve()
        if not root.is_relative_to(base):
            raise ValueError(f"robot path escapes base directory: {root}")

        return cls(root, capped_lanes={"channels": settings.FLEET_SPOOL_MAX_BYTES})

    # -- replay ------------------------------------------------------------------

    def pending(self, lane: str) -> Iterator[tuple[int, dict]]:
        """Iterate over all records in a lane with seq > watermark.

        Args:
            lane: Lane name.

        Yields:
            Tuples of (seq, payload) in ascending seq order.

        Raises:
            SpoolCorruptError: A non-final segment line, or a non-final segment's
                final line, fails to parse (mid-file corruption).

        Note:
            This is a read path: it never creates a lane directory. An ack for a
            yielded record may arrive on the publisher's callback thread only after
            that record has been yielded here; the segment list is captured once, at
            the start of iteration, so segments rolled or pruned mid-iteration by a
            concurrent ack do not change what this call sees. A segment that
            existed at snapshot time but is unlinked by a concurrent
            eviction before this loop reaches it (Codex review) is treated
            as "already gone" and skipped, rather than raising
            FileNotFoundError and aborting the rest of the replay.

        """
        watermark = self._watermark(lane)
        segments = self._segments(lane, create=False)
        for i, segment in enumerate(segments):
            is_final = i == len(segments) - 1
            try:
                records, _, _ = _scan_segment(segment, lane=lane, tolerate_torn_tail=is_final)
            except FileNotFoundError:
                continue  # concurrently evicted between the snapshot and this read
            for record in records:
                if record["seq"] > watermark:
                    yield record["seq"], record["payload"]
