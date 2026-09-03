"""Deterministic fleet health report: `build_health_report` and its ten checks.

Spec fleet-step8 Task 6. This module is PURE: it reads whatever the caller
(Task 7's health-publishing closure) hands it in a `HealthInputs` and returns
a plain, JSON-able `summary` dict plus the `HealthSnapshot` the next call
should pass back as `previous` -- no threads, no disk, no network, no clock
reads (`now` is always a caller-supplied float, `time.time()` never appears
here). `FleetService`, the spool, the event engine and the artifact store are
all gathered by the caller; this module only ever sees their already-reduced
numbers.

Ten checks run every time, each producing one `{"name", "status", "metrics"}`
entry (`+ "reason"` iff `status != "pass"`). Several checks are *delta*
checks -- they care whether a cumulative counter (Spool eviction count,
router/emitter queue drops, heartbeat spool-append failures, publisher
reconnects) has grown *since the previous report*, not its raw value, so a
one-time blip doesn't warn forever. `HealthSnapshot` carries exactly those
five cumulative counters; `previous=None` (the boot report, or a caller that
never persisted one) is treated as a snapshot of all-zeros, so first-report
deltas equal the cumulative values themselves -- see `snapshot_from`.

`HealthInputs.events_counters` is expected to already be the CALLER's merge
of `EventEngine.counters()` (`fired`, `suppressed`, a per-rule
`predicate_errors` dict) with the event emitter's own queue depth/dropped
count (Task 7): `{"queue_depth", "dropped", "predicate_errors" (a single
int -- the caller's sum across rules), "fired", "suppressed"}`. This module
only reads those five flat keys.
"""

import dataclasses
import datetime

from src.sink.publish.heartbeat import bagel_version
from src.sink.publish.identity import _RENEWAL_WINDOW_S

CHECK_STATUSES = ("pass", "warn", "fail", "skipped")

# Thresholds are code, not settings: they define what the report MEANS, so
# changing one is a behavior change, not a deployment knob.
DISK_FAIL_BYTES = 536_870_912
DISK_WARN_BYTES = 2_147_483_648
SPOOL_WARN_FRACTION = 0.8
ARTIFACTS_WARN_FRACTION = 0.8
TOPIC_STALE_AFTER_S = 300.0
EVENTS_BACKLOG_WARN = 1_000

_SCHEMA_REV = 1
_EMPTY_LANE: dict = {"bytes": 0, "pending": 0, "last_seq": 0, "acked_seq": 0, "evicted": 0}


@dataclasses.dataclass
class HealthInputs:
    """Everything one health report reads, gathered by the caller (pure boundary).

    `status` mirrors `FleetService.status()`'s dict shape exactly (see that
    method's docstring): top-level `online`/`backoff`/`reconnects`/
    `router_alive`/`router_error`/`heartbeat_alive`/`heartbeat_error`/
    `heartbeat_spool_failures`, plus nested `queue` (`depth`/`dropped`) and
    `spool` (lane name -> `bytes`/`pending`/`last_seq`/`acked_seq`/`evicted`).

    `topic_last_seen` maps each tapped topic to its
    `TopicBufferWriter.last_timestamp_seconds` (`None` if never received --
    source timestamps may be sim time, so this is advisory, not wall-clock
    exact; see the `topic_staleness` check).

    `artifacts` is `ArtifactStore.stats()`'s `{"bytes", "files"}`, or `{}`
    when no artifact store is wired (no artifact-bearing event rules
    configured) -- the `artifacts` check treats an empty dict as "skip, not
    zero usage".
    """

    status: dict
    topic_last_seen: dict[str, float | None]
    cert_expires_at: str | None
    enrolled: bool
    disk_free_bytes: int
    spool_cap_bytes: int
    artifacts: dict
    artifacts_cap_bytes: int
    # events_counters: the CALLER's flattened merge of EventEngine.counters()
    # + the emitter's queue stats: {queue_depth, dropped, predicate_errors,
    # fired, suppressed} -- predicate_errors must be a SINGLE INT (sum across
    # rules); EventEngine.counters() returns a per-rule dict, so the caller
    # sums it. Passing the raw dict through crashes the events_pipeline check.
    events_counters: dict
    uptime_s: float


@dataclasses.dataclass
class HealthSnapshot:
    """The cumulative counters this report's deltas are computed against.

    `snapshot_from` builds one from a `HealthInputs`; the caller persists the
    `build_health_report` return value and passes it back as `previous` on
    the next call so delta checks (queue/events drops, spool evictions,
    heartbeat spool failures, reconnects) measure "since last report" rather
    than "since boot".
    """

    queue_dropped: int
    events_queue_dropped: int
    spool_evicted: int
    heartbeat_spool_failures: int
    reconnects: int
    taken_at: float


def snapshot_from(inputs: HealthInputs, now: float) -> HealthSnapshot:
    """Extract the five cumulative counters `build_health_report` diffs against."""
    queue = inputs.status.get("queue", {})
    channels = inputs.status.get("spool", {}).get("channels", _EMPTY_LANE)
    return HealthSnapshot(
        queue_dropped=queue.get("dropped", 0),
        events_queue_dropped=inputs.events_counters.get("dropped", 0),
        spool_evicted=channels.get("evicted", 0),
        heartbeat_spool_failures=inputs.status.get("heartbeat_spool_failures", 0),
        reconnects=inputs.status.get("reconnects", 0),
        taken_at=now,
    )


def _entry(name: str, status: str, metrics: dict, reason: str | None = None) -> dict:
    entry = {"name": name, "status": status, "metrics": metrics}
    if status != "pass":
        entry["reason"] = reason
    return entry


def _check_connection(
    inputs: HealthInputs, baseline: HealthSnapshot, snapshot: HealthSnapshot
) -> dict:
    """Router liveness/connectivity: dead router fails, offline-but-alive warns."""
    status = inputs.status
    metrics = {
        "online": status.get("online", False),
        "backoff": status.get("backoff"),
        "reconnects": snapshot.reconnects,
        "reconnects_delta": snapshot.reconnects - baseline.reconnects,
    }
    if not status.get("router_alive", False):
        reason = status.get("router_error") or "router thread not alive"
        return _entry("connection", "fail", metrics, reason=reason)
    if not status.get("online", False):
        return _entry("connection", "warn", metrics, reason="offline-retrying")
    return _entry("connection", "pass", metrics)


def _check_queue(inputs: HealthInputs, baseline: HealthSnapshot, snapshot: HealthSnapshot) -> dict:
    """Router sample queue: warn only while drops are actively growing."""
    queue = inputs.status.get("queue", {})
    dropped_delta = snapshot.queue_dropped - baseline.queue_dropped
    metrics = {
        "depth": queue.get("depth", 0),
        "dropped": snapshot.queue_dropped,
        "dropped_delta": dropped_delta,
    }
    if dropped_delta > 0:
        reason = f"dropped {dropped_delta} sample(s) since last check"
        return _entry("queue", "warn", metrics, reason=reason)
    return _entry("queue", "pass", metrics)


def _check_events_pipeline(
    inputs: HealthInputs, baseline: HealthSnapshot, snapshot: HealthSnapshot
) -> dict:
    """Event emitter: warn while its queue is actively dropping, or on any predicate error."""
    counters = inputs.events_counters
    dropped_delta = snapshot.events_queue_dropped - baseline.events_queue_dropped
    predicate_errors = counters.get("predicate_errors", 0)
    metrics = {
        "queue_depth": counters.get("queue_depth", 0),
        "dropped": snapshot.events_queue_dropped,
        "dropped_delta": dropped_delta,
        "predicate_errors": predicate_errors,
        "fired": counters.get("fired", 0),
        "suppressed": counters.get("suppressed", 0),
    }
    if dropped_delta > 0 or predicate_errors > 0:
        reasons = []
        if dropped_delta > 0:
            reasons.append(f"dropped {dropped_delta} event(s) since last check")
        if predicate_errors > 0:
            reasons.append(f"{predicate_errors} predicate error(s)")
        return _entry("events_pipeline", "warn", metrics, reason="; ".join(reasons))
    return _entry("events_pipeline", "pass", metrics)


def _check_spool(inputs: HealthInputs, baseline: HealthSnapshot, snapshot: HealthSnapshot) -> dict:
    """Channels spool lane: an eviction this period is data loss (fail); high usage warns."""
    channels = inputs.status.get("spool", {}).get("channels", _EMPTY_LANE)
    cap_bytes = inputs.spool_cap_bytes
    lane_bytes = channels.get("bytes", 0)
    evicted_delta = snapshot.spool_evicted - baseline.spool_evicted
    metrics = {
        "bytes": lane_bytes,
        "pending": channels.get("pending", 0),
        "evicted": snapshot.spool_evicted,
        "evicted_delta": evicted_delta,
        "cap_bytes": cap_bytes,
    }
    if evicted_delta > 0:
        return _entry(
            "spool", "fail", metrics, reason=f"evicted {evicted_delta} record(s) since last check"
        )
    if lane_bytes > SPOOL_WARN_FRACTION * cap_bytes:
        reason = f"{lane_bytes} bytes exceeds {SPOOL_WARN_FRACTION:.0%} of the {cap_bytes}-byte cap"
        return _entry("spool", "warn", metrics, reason=reason)
    return _entry("spool", "pass", metrics)


def _check_events_backlog(inputs: HealthInputs) -> dict:
    """Events spool lane pending count: high backlog means events aren't reaching the cloud."""
    events_lane = inputs.status.get("spool", {}).get("events", _EMPTY_LANE)
    pending = events_lane.get("pending", 0)
    metrics = {"pending": pending}
    if pending > EVENTS_BACKLOG_WARN:
        return _entry(
            "events_backlog",
            "warn",
            metrics,
            reason=f"{pending} pending events exceeds the {EVENTS_BACKLOG_WARN} warn threshold",
        )
    return _entry("events_backlog", "pass", metrics)


def _check_disk(inputs: HealthInputs) -> dict:
    """Free disk: strictly below the fail threshold fails; strictly below warn warns.

    `<`, not `<=`: exactly `DISK_FAIL_BYTES` free bytes does NOT fail -- it
    still satisfies `< DISK_WARN_BYTES`, so it warns instead.
    """
    free = inputs.disk_free_bytes
    metrics = {"disk_free_bytes": free}
    if free < DISK_FAIL_BYTES:
        reason = f"{free} bytes free is below {DISK_FAIL_BYTES}"
        return _entry("disk", "fail", metrics, reason=reason)
    if free < DISK_WARN_BYTES:
        reason = f"{free} bytes free is below {DISK_WARN_BYTES}"
        return _entry("disk", "warn", metrics, reason=reason)
    return _entry("disk", "pass", metrics)


def _check_certificate(inputs: HealthInputs, now: float) -> dict:
    """Certificate expiry: unenrolled skips; past fails; within the renewal window warns."""
    if not inputs.enrolled:
        return _entry(
            "certificate", "skipped", {"expires_at": None, "days_left": None}, reason="not enrolled"
        )
    expires_at = inputs.cert_expires_at
    try:
        if not isinstance(expires_at, str):
            raise ValueError("cert_expires_at is not a string")
        expires_dt = datetime.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if expires_dt.tzinfo is None:
            raise ValueError("cert_expires_at has no timezone")
    except ValueError:
        return _entry(
            "certificate",
            "warn",
            {"expires_at": expires_at, "days_left": None},
            reason=f"unparsable cert_expires_at: {expires_at!r}",
        )
    seconds_left = expires_dt.timestamp() - now
    metrics = {"expires_at": expires_at, "days_left": seconds_left / 86400.0}
    if seconds_left < 0:
        return _entry("certificate", "fail", metrics, reason="certificate has expired")
    if seconds_left <= _RENEWAL_WINDOW_S:
        return _entry("certificate", "warn", metrics, reason="certificate renewal due soon")
    return _entry("certificate", "pass", metrics)


def _check_topic_staleness(inputs: HealthInputs, now: float) -> dict:
    """Per-tapped-topic recency: no tapped topics skips; any never-seen/stale topic warns.

    Advisory, not authoritative -- source timestamps may be sim time, so a
    robot legitimately replaying old data (or legitimately idle) can trip
    this even though nothing is actually wrong.
    """
    topics = inputs.topic_last_seen
    if not topics:
        return _entry(
            "topic_staleness", "skipped", {"stale": [], "topics": 0}, reason="no tapped topics"
        )
    cutoff = now - TOPIC_STALE_AFTER_S
    stale = sorted(
        name for name, last_seen in topics.items() if last_seen is None or last_seen < cutoff
    )
    metrics = {"stale": stale, "topics": len(topics)}
    if stale:
        return _entry(
            "topic_staleness", "warn", metrics, reason=f"stale topic(s): {', '.join(stale)}"
        )
    return _entry("topic_staleness", "pass", metrics)


def _check_heartbeat(
    inputs: HealthInputs, baseline: HealthSnapshot, snapshot: HealthSnapshot
) -> dict:
    """Heartbeat thread liveness: dead fails; a growing spool-failure count or an error warns."""
    status = inputs.status
    spool_failures_delta = snapshot.heartbeat_spool_failures - baseline.heartbeat_spool_failures
    heartbeat_error = status.get("heartbeat_error")
    metrics = {
        "alive": status.get("heartbeat_alive", False),
        "spool_failures": snapshot.heartbeat_spool_failures,
        "spool_failures_delta": spool_failures_delta,
    }
    if not status.get("heartbeat_alive", False):
        return _entry("heartbeat", "fail", metrics, reason="heartbeat thread not alive")
    if spool_failures_delta > 0 or heartbeat_error:
        reason = heartbeat_error or f"{spool_failures_delta} spool failure(s) since last check"
        return _entry("heartbeat", "warn", metrics, reason=reason)
    return _entry("heartbeat", "pass", metrics)


def _check_artifacts(inputs: HealthInputs) -> dict:
    """Artifact store usage: no store wired skips; high usage warns."""
    artifacts = inputs.artifacts
    if not artifacts:
        return _entry(
            "artifacts",
            "skipped",
            {"bytes": 0, "files": 0, "cap_bytes": inputs.artifacts_cap_bytes},
            reason="no artifact rules",
        )
    lane_bytes = artifacts.get("bytes", 0)
    cap_bytes = inputs.artifacts_cap_bytes
    metrics = {"bytes": lane_bytes, "files": artifacts.get("files", 0), "cap_bytes": cap_bytes}
    if lane_bytes > ARTIFACTS_WARN_FRACTION * cap_bytes:
        pct = ARTIFACTS_WARN_FRACTION
        reason = f"{lane_bytes} bytes exceeds {pct:.0%} of the {cap_bytes}-byte cap"
        return _entry("artifacts", "warn", metrics, reason=reason)
    return _entry("artifacts", "pass", metrics)


_RANK = {"fail": 2, "warn": 1}


def verdict(checks: list[dict]) -> str:
    """One deterministic summary line for a check list -- exact format pinned by test.

    Any `fail` or `warn` present -> `f"{worst}: {sorted names at that worst
    status}"` (`fail` outranks `warn`). Otherwise (only `pass`/`skipped`
    remain) -> `"all {n} checks pass"`, or `"all {n} checks pass ({k}
    skipped)"` when `k` were skipped. `skipped` never drives the verdict --
    it marks a check that could not apply, not a problem.
    """
    worst_rank = max((_RANK.get(c["status"], 0) for c in checks), default=0)
    if worst_rank > 0:
        worst = next(status for status, rank in _RANK.items() if rank == worst_rank)
        names = sorted(c["name"] for c in checks if c["status"] == worst)
        return f"{worst}: {', '.join(names)}"
    n = len(checks)
    skipped = sum(1 for c in checks if c["status"] == "skipped")
    if skipped:
        return f"all {n} checks pass ({skipped} skipped)"
    return f"all {n} checks pass"


def build_health_report(
    inputs: HealthInputs, previous: HealthSnapshot | None, *, now: float
) -> tuple[dict, HealthSnapshot]:
    """Build one health report summary plus the snapshot the next call should chain from.

    `previous=None` (boot report, or no persisted snapshot) is treated as a
    zeroed baseline -- see `HealthSnapshot`'s docstring -- so every delta
    check's first-ever report shows its delta equal to the cumulative
    counter itself.
    """
    snapshot = snapshot_from(inputs, now)
    baseline = previous if previous is not None else HealthSnapshot(0, 0, 0, 0, 0, now)
    checks = [
        _check_connection(inputs, baseline, snapshot),
        _check_queue(inputs, baseline, snapshot),
        _check_events_pipeline(inputs, baseline, snapshot),
        _check_spool(inputs, baseline, snapshot),
        _check_events_backlog(inputs),
        _check_disk(inputs),
        _check_certificate(inputs, now),
        _check_topic_staleness(inputs, now),
        _check_heartbeat(inputs, baseline, snapshot),
        _check_artifacts(inputs),
    ]
    summary = {
        "schema_rev": _SCHEMA_REV,
        "source": {
            "component": "bagel",
            "bagel_version": bagel_version(),
            "uptime_s": inputs.uptime_s,
        },
        "checks": checks,
        "verdict": verdict(checks),
    }
    return summary, snapshot
