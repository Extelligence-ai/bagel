"""Heartbeat: liveness payload building and the wall-clock publishing thread.

Spec §3. `bagel_version()` probes `importlib.metadata` for the installed
distribution first (works for a `uv sync`/pip-installed image); when that
distribution metadata is absent -- as in a source checkout run via `uv run`
without `uv sync --package/-e` having registered it, which is the live path
in this worktree -- it falls back to parsing `pyproject.toml`'s
`[project].version` directly. Either miss returns `"unknown"` rather than
raising: a heartbeat must go out even if its own version string is a mystery.
"""

import importlib.metadata
import logging
import pathlib
import shutil
import threading
import time
from collections.abc import Callable

import tomllib

from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.spool import Spool

HEARTBEAT_INTERVAL_S = 30.0

_DISTRIBUTION_NAME = "bagel"


def _pyproject_version() -> str:
    """Read `[project].version` from the repo's pyproject.toml (fallback path)."""
    root = pathlib.Path(__file__).resolve().parents[3]
    data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def bagel_version() -> str:
    """Return the running Bagel version, or "unknown" if neither source works."""
    try:
        return importlib.metadata.version(_DISTRIBUTION_NAME)
    except importlib.metadata.PackageNotFoundError:
        pass
    try:
        return _pyproject_version()
    except Exception:
        return "unknown"


def disk_free(path: str | pathlib.Path) -> int:
    """Free bytes on the filesystem containing `path`."""
    return shutil.disk_usage(path).free


def _lane_field(stat: object, field: str) -> int:
    """Read one numeric field off a lane-stats value that may be a dict or an object."""
    if isinstance(stat, dict):
        return stat.get(field, 0)
    return getattr(stat, field, 0)


def build_heartbeat(  # noqa: PLR0913
    *,
    started_at: float,
    subscriptions: list[str],
    channels_active: int,
    queue_depth: int,
    queue_dropped: int,
    spool_stats: dict,
    disk_free_bytes: int,
    reconnects: int,
    now: float | None = None,
) -> dict:
    """Build the §3 heartbeat payload.

    `spool_stats` maps lane name -> a value exposing `bytes`/`pending`/
    `evicted` (either as dict keys or attributes, e.g. `Spool.stats()`'s
    `LaneStats`); this aggregates the channels/events/heartbeat lanes into
    one `spool` object by summing each field. `cert_expires_at` is always
    `None` here -- identity/enrollment lands in step 6.
    """
    t = now if now is not None else time.time()
    return {
        "v": 1,
        "t": t,
        "online": True,
        "bagel_version": bagel_version(),
        "uptime_s": t - started_at,
        "subscriptions": list(subscriptions),
        "channels_active": channels_active,
        "queue": {"depth": queue_depth, "dropped": queue_dropped},
        "spool": {
            "bytes": sum(_lane_field(s, "bytes") for s in spool_stats.values()),
            "pending": sum(_lane_field(s, "pending") for s in spool_stats.values()),
            "evicted": sum(_lane_field(s, "evicted") for s in spool_stats.values()),
        },
        "disk_free_bytes": disk_free_bytes,
        "cert_expires_at": None,
        "reconnects": reconnects,
    }


class HeartbeatThread(threading.Thread):
    """Publishes a fresh heartbeat payload on a fixed interval.

    Follows the thread-lifecycle precedent documented in `service.py`:
    daemon=True, a `threading.Event` waited on with the tick interval as its
    timeout (so `stop()` returns as soon as the event is set rather than
    after a full interval elapses), and a bounded `join()` in `stop()`.

    A publish failure never kills the thread: heartbeat is the never-drop
    lane (spec §4), so on `PublishError` the payload is appended to the
    spool's "heartbeat" lane instead (best-effort -- a spool failure here is
    swallowed too, since a heartbeat thread that dies is worse than one that
    occasionally fails to record a missed beat).
    """

    def __init__(
        self,
        publisher: Publisher,
        payload_factory: Callable[[], dict],
        interval_s: float = HEARTBEAT_INTERVAL_S,
        spool: Spool | None = None,
    ) -> None:
        """Wire the publisher, payload builder, and optional spool; does not start the thread."""
        super().__init__(daemon=True)
        self._publisher = publisher
        self._payload_factory = payload_factory
        self._interval_s = interval_s
        self._spool = spool
        self._stop_event = threading.Event()

    def run(self) -> None:
        """Tick immediately on start, then every `interval_s`, until stop() is called."""
        while not self._stop_event.is_set():
            self._tick()
            self._stop_event.wait(self._interval_s)

    def _tick(self) -> None:
        """One heartbeat: build the payload, publish it, spool it on failure."""
        payload = self._payload_factory()
        try:
            self._publisher.publish_heartbeat(payload)
        except PublishError:
            if self._spool is not None:
                try:
                    seq = self._spool.next_seq("heartbeat")
                    self._spool.append("heartbeat", seq, payload)
                except Exception:
                    logging.getLogger(__name__).debug(
                        "heartbeat spool append failed", exc_info=True
                    )

    def stop(self) -> None:
        """Signal the loop to stop and join with a bounded timeout."""
        self._stop_event.set()
        self.join(timeout=5)
