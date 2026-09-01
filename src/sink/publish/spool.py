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
"""

import dataclasses
import json
import os
import pathlib
import tempfile
from collections.abc import Iterator

import filelock

SEGMENT_MAX_BYTES = 4 * 1024 * 1024


class SpoolError(Exception):
    """Raised when the spool cannot honor a request."""


class SpoolFullError(SpoolError):
    """Raised when a never-drop lane cannot write (disk full or failing)."""


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


def _parse_lines(segment: pathlib.Path, *, tolerate_torn_tail: bool = False) -> Iterator[dict]:
    """Parse JSONL lines from a segment, optionally tolerating a torn final line.

    Args:
        segment: Path to segment file.
        tolerate_torn_tail: If True, ignore JSONDecodeError on the final line only.
            Corruption in any non-final line still raises.

    Yields:
        Parsed JSON objects.

    Raises:
        JSONDecodeError: If any non-final line fails to parse, or if any line fails
            and tolerate_torn_tail is False.

    """
    lines = segment.read_text(encoding="utf-8").splitlines()
    for i, line in enumerate(lines):
        is_final = i == len(lines) - 1
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            if is_final and tolerate_torn_tail:
                # Skip the torn final line; preceding lines win
                continue
            raise


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
        self._lock = filelock.FileLock(str(self._root / ".lock"))
        self._last_seq: dict[str, int] = {}
        self._evicted: dict[str, int] = {}

    # -- paths ---------------------------------------------------------------

    def _lane_dir(self, lane: str) -> pathlib.Path:
        path = self._root / lane
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _segments(self, lane: str) -> list[pathlib.Path]:
        return sorted(self._lane_dir(lane).glob("segment-*.jsonl"))

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
        segments = self._segments(lane)
        if not segments:
            return self._watermark(lane)
        last = 0
        for record in _parse_lines(segments[-1], tolerate_torn_tail=True):
            last = record["seq"]
        return max(last, self._watermark(lane))

    def next_seq(self, lane: str) -> int:
        """Get the next sequence number for a lane.

        Args:
            lane: Lane name.

        Returns:
            Next monotonic sequence number (1-based).

        """
        with self._lock:
            if lane not in self._last_seq:
                self._last_seq[lane] = self._scan_last_seq(lane)
            return self._last_seq[lane] + 1

    # -- append ----------------------------------------------------------------

    def append(self, lane: str, seq: int, payload: dict) -> None:
        """Append a record to a lane.

        Args:
            lane: Lane name.
            seq: Sequence number (must be > last written seq).
            payload: JSON-serializable record.

        Raises:
            ValueError: If seq is not monotonic.
            SpoolFullError: If lane is never-capped and write fails.
            SpoolError: If lane is capped and write fails.

        """
        with self._lock:
            if lane not in self._last_seq:
                self._last_seq[lane] = self._scan_last_seq(lane)
            if seq <= self._last_seq[lane]:
                raise ValueError(
                    f"seq must be monotonic: got {seq}, last was {self._last_seq[lane]}"
                )
            line = json.dumps({"seq": seq, "payload": payload}) + "\n"
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
        acks in order); ``seq`` at or below it is an idempotent no-op.
        """
        with self._lock:
            marks = self._watermarks()
            if seq <= marks.get(lane, 0):
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
        segments = self._segments(lane)
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

        Returns:
            Dict mapping lane name to LaneStats.

        """
        with self._lock:
            marks = self._watermarks()
            out: dict[str, LaneStats] = {}
            lanes = {p.name for p in self._root.iterdir() if p.is_dir()}
            for lane in sorted(lanes):
                segments = self._segments(lane)
                acked = marks.get(lane, 0)
                last = self._last_seq.get(lane) or self._scan_last_seq(lane)
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

        """
        watermark = self._watermark(lane)
        segments = self._segments(lane)
        for i, segment in enumerate(segments):
            is_final = i == len(segments) - 1
            for record in _parse_lines(segment, tolerate_torn_tail=is_final):
                if record["seq"] > watermark:
                    yield record["seq"], record["payload"]
