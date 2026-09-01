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

import json
import pathlib
from collections.abc import Iterator

import filelock

SEGMENT_MAX_BYTES = 4 * 1024 * 1024


class SpoolError(Exception):
    """Raised when the spool cannot honor a request."""


class SpoolFullError(SpoolError):
    """Raised when a never-drop lane cannot write (disk full or failing)."""


def _segment_name(first_seq: int) -> str:
    return f"segment-{first_seq:016d}.jsonl"


def _first_seq_of(path: pathlib.Path) -> int:
    return int(path.stem.split("-", 1)[1])


class Spool:
    """One robot's outbox: lanes of segments plus an acked watermark."""

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
        for line in segments[-1].read_text(encoding="utf-8").splitlines():
            last = json.loads(line)["seq"]
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
            if active is None or active.stat().st_size + len(line) > SEGMENT_MAX_BYTES:
                active = self._lane_dir(lane) / _segment_name(seq)
            try:
                with open(active, "a", encoding="utf-8") as handle:
                    handle.write(line)
            except OSError as exc:
                if lane not in self._capped:
                    raise SpoolFullError(f"lane '{lane}': {exc}") from exc
                raise SpoolError(f"lane '{lane}': {exc}") from exc
            self._last_seq[lane] = seq

    # -- replay ------------------------------------------------------------------

    def pending(self, lane: str) -> Iterator[tuple[int, dict]]:
        """Iterate over all records in a lane with seq > watermark.

        Args:
            lane: Lane name.

        Yields:
            Tuples of (seq, payload) in ascending seq order.

        """
        watermark = self._watermark(lane)
        for segment in self._segments(lane):
            for line in segment.read_text(encoding="utf-8").splitlines():
                record = json.loads(line)
                if record["seq"] > watermark:
                    yield record["seq"], record["payload"]
