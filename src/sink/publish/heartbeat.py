"""Heartbeat: liveness payload building and the wall-clock publishing thread.

Spec §3. `bagel_version()` probes `importlib.metadata` for the installed
distribution first (works for a `uv sync`/pip-installed image); when that
distribution metadata is absent -- as in a source checkout run via `uv run`
without `uv sync --package/-e` having registered it, which is the live path
in this worktree -- it falls back to parsing `pyproject.toml`'s
`[project].version` directly with a small zero-dependency regex, NOT
`tomllib` (Codex review: `tomllib` is stdlib only on Python 3.11+, and this
module must import cleanly on the 3.10-based ros2-humble/iron images too --
a module-level `import tomllib` broke CI's image import-probe on those).
Either miss returns `"unknown"` rather than raising: a heartbeat must go out
even if its own version string is a mystery.
"""

import importlib.metadata
import logging
import pathlib
import re
import shutil
import threading
import time
from collections.abc import Callable

from src.sink.publish.provenance import build_provenance
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.spool import Spool

HEARTBEAT_INTERVAL_S = 30.0

_DISTRIBUTION_NAME = "bagel"

_VERSION_LINE_RE = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)


def _pyproject_version(root: pathlib.Path | None = None) -> str:
    """Read `[project].version` from a pyproject.toml (fallback path).

    Parsed with a plain regex rather than `tomllib` -- zero dependencies, and
    it works on every Python this repo targets (not just 3.11+). `root`
    defaults to the repo root inferred from this file's location; a caller
    (tests) may pass a different directory containing its own pyproject.toml.
    """
    root = root if root is not None else pathlib.Path(__file__).resolve().parents[3]
    text = (root / "pyproject.toml").read_text(encoding="utf-8")
    match = _VERSION_LINE_RE.search(text)
    if match is None:
        raise ValueError(f'no version = "..." line found in {root / "pyproject.toml"}')
    return match.group(1)


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
    cert_expires_at: str | None = None,
) -> dict:
    """Build the §3 heartbeat payload.

    `spool_stats` maps lane name -> a value exposing `bytes`/`pending`/
    `evicted` (either as dict keys or attributes, e.g. `Spool.stats()`'s
    `LaneStats`); this aggregates the channels/events/heartbeat lanes into
    one `spool` object by summing each field. `cert_expires_at` defaults to
    `None` (an unenrolled robot, or a caller with no identity wired) and is
    otherwise passed straight through from the caller's `Identity.expires_at`
    -- `FleetService` supplies it once constructed with an `identity`.
    """
    t = now if now is not None else time.time()
    payload = {
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
        "cert_expires_at": cert_expires_at,
        "reconnects": reconnects,
    }
    build = build_provenance()
    if build is not None:
        payload["build"] = build
    return payload


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

    An optional `renewal_check` callable is invoked once per tick, before
    the payload is built -- it exists purely as a convenient, already-running
    clock for certificate renewal (`FleetService` wires its own
    `identity.should_attempt_renewal`/`identity.renew` closure in here when
    constructed with an `identity`). It runs inside its own try/except,
    entirely separate from the payload-factory/publish/spool handling below:
    an exception from it is logged at WARNING and otherwise ignored -- it
    must never be able to skip a beat or kill this thread.
    """

    def __init__(
        self,
        publisher: Publisher,
        payload_factory: Callable[[], dict],
        interval_s: float = HEARTBEAT_INTERVAL_S,
        spool: Spool | None = None,
        renewal_check: Callable[[], None] | None = None,
    ) -> None:
        """Wire the publisher, payload builder, optional spool, and optional renewal hook.

        Does not start the thread.
        """
        super().__init__(daemon=True)
        self._publisher = publisher
        self._payload_factory = payload_factory
        self._interval_s = interval_s
        self._spool = spool
        self._renewal_check = renewal_check
        self._stop_event = threading.Event()
        self._spool_failures = 0
        self._last_error: str | None = None

    def run(self) -> None:
        """Tick immediately on start, then every `interval_s`, until stop() is called."""
        while not self._stop_event.is_set():
            self._tick()
            self._stop_event.wait(self._interval_s)

    def _tick(self) -> None:
        """One heartbeat: build the payload, publish it, spool it on failure.

        The `payload_factory()` call is its own try/except: it can raise
        (e.g. `Spool.stats()` -> `SpoolCorruptError`, `disk_free()` on a
        missing directory) and an uncaught exception here would unwind
        `run()`'s loop and silently kill the liveness thread with no
        visibility anywhere. So a factory exception logs at WARNING (with
        the error) and skips this beat entirely -- `last_error` is set and
        the thread lives to try again next interval. `last_error` is
        cleared as soon as a beat's payload is built successfully; it does
        not track publish failures, which are the separate, normal
        offline path below.

        A `PublishError` (the broker is unreachable) is normal, expected
        offline behavior -- logged at DEBUG. A failure spooling the payload
        to the never-drop "heartbeat" lane afterward is not: per spec §4
        that lane must never silently drop, so a `SpoolError` (e.g.
        `SpoolFullError` -- disk full) or any other exception from the spool
        path logs at WARNING (with the lane and reason) and increments
        `spool_failures` so it is visible to an operator via
        `FleetService.status()`, instead of vanishing at DEBUG. Either way
        the thread never dies and no exception escapes this method.

        `renewal_check()` (if wired) runs first, in its own try/except --
        see the class docstring. Its outcome (and any exception from it) has
        no bearing on whether this beat's payload is built or published.
        """
        if self._renewal_check is not None:
            try:
                self._renewal_check()
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "renewal_check hook failed: %s", exc, exc_info=True
                )
        try:
            payload = self._payload_factory()
        except Exception as exc:
            self._last_error = f"heartbeat payload factory failed: {exc!r}"
            logging.getLogger(__name__).warning(
                "heartbeat payload factory failed; skipping this beat: %s", exc, exc_info=True
            )
            return
        self._last_error = None
        try:
            self._publisher.publish_heartbeat(payload)
        except PublishError:
            logging.getLogger(__name__).debug("heartbeat publish failed", exc_info=True)
            if self._spool is not None:
                try:
                    # `append_next`, not a separate `next_seq()` + `append()`
                    # pair (Codex round 3 P1 follow-up, comment 3924082774):
                    # that shape left a gap a concurrent writer (e.g. a
                    # selftest run) could land in, allocating a seq that
                    # collided with what append() then tried to use. This
                    # `except Exception` already caught that `ValueError`
                    # (so it was never a thread-killer here, unlike the
                    # router's unguarded batch-flush path -- see
                    # `router.py`), but it still silently dropped this
                    # beat's heartbeat from the never-drop lane on a
                    # spurious collision. The heartbeat payload itself
                    # carries no embedded `seq` field (spec §8 -- unlike
                    # channels/events), so the `build` callable is trivial.
                    self._spool.append_next("heartbeat", lambda _seq: payload)
                except Exception as exc:
                    self._spool_failures += 1
                    logging.getLogger(__name__).warning(
                        "heartbeat spool append failed on lane 'heartbeat': %s", exc, exc_info=True
                    )

    def stop(self) -> None:
        """Signal the loop to stop and join with a bounded timeout."""
        self._stop_event.set()
        self.join(timeout=5)

    @property
    def spool_failures(self) -> int:
        """Count of failed attempts to spool a heartbeat payload after a publish failure."""
        return self._spool_failures

    @property
    def alive(self) -> bool:
        """Whether the thread is running and has hit no fatal state.

        Mirrors `StreamRouter.alive`'s visibility pattern, but the payload
        factory fix above means `_tick` has no remaining fatal path -- a
        factory failure is now caught and skipped rather than escaping
        `run()`'s loop -- so this is simply `is_alive()`.
        """
        return self.is_alive()

    @property
    def last_error(self) -> str | None:
        """The most recent payload-factory failure, if the last beat skipped one.

        Set when a beat's `payload_factory()` call raises (that beat is
        skipped); cleared as soon as a later beat builds its payload
        successfully. Independent of publish/spool outcomes -- those are
        tracked separately (see `spool_failures`).
        """
        return self._last_error
