"""Fleet control-plane operations: the tool-facing layer over `FleetService`.

Step 7's MCP tools are thin passthroughs to this module (spec §7) -- this is
where the four-state sanity ruling lives: every operation here behaves
sanely whether the `fleet` dependency group is installed, whether
`settings.FLEET_ENABLED` is on, whether this robot is enrolled, and whether a
`FleetService` is currently running.

`fleet_status()` is the one exception to `require_fleet()`: per the binding
ruling, it calls NO gate at all, because it IS the tool that reports on
those first two states rather than assuming them -- a caller diagnosing "why
won't fleet streaming start" needs an answer even when it's not installed or
disabled, not an exception. `unenroll_identity()` is the other exception
(ruling): unenrolling only makes the fleet subsystem MORE inert, so it must
work even with `FLEET_ENABLED=0`. Every other operation here calls
`require_fleet()` first, so `FLEET_ENABLED=0` raises `FleetDisabledError`
before the live-service holder (`src.sink.startup.fleet_service()`) is ever
touched.

`connect`, `config`, `spool`, and `mqtt` are all imported at module scope
here (alongside `startup` and `identity`) -- none of them import
paho/cryptography eagerly either (see each module's own docstring), so
doing so does not trip the package's no-eager-import invariant; a lazy
regression test for THIS module lives in `test_control.py` alongside the
existing gate/service ones.

`_control_lock` (I2 ruling) serializes every mutating operation here --
`stream_topics`, `stop_streams`, `unenroll_identity`, `pause_streaming`,
`resume_streaming`, `enroll_identity` -- since an MCP client may invoke
tools concurrently and stop-old/start-new is not itself atomic across two
interleaved callers. `fleet_status()` intentionally never acquires it: it
is read-only and must never block on a slow mutating call.
"""

import importlib.util
import logging
import os
import pathlib
import stat
import tempfile
import threading
from collections.abc import Callable

import yaml

from settings import settings
from src.sink import base, startup
from src.sink.publish import (
    EnrollmentError,
    FleetNotEnrolledError,
    StreamConfigError,
    config,
    identity,
    require_fleet,
)
from src.sink.publish.connect import resolve_publisher_kwargs
from src.sink.publish.mqtt import MqttPublisher
from src.sink.publish.service import FleetService
from src.sink.publish.spool import Spool

# Serializes tool-driven lifecycle transitions (I2 ruling): MCP workers may
# run tools concurrently, and stop-old/start-new (`_restart_service`) is not
# itself atomic across two interleaved callers. `fleet_status()` is
# intentionally left unlocked -- it is read-only and must never block on a
# slow mutating call.
_control_lock = threading.Lock()

# Honest-reporting ruling (Codex round 3, P1a): event rules are accepted,
# validated, merged, restarted against, and persisted to the manifest just
# like channel rules -- but the on-robot runtime that would actually
# evaluate an event predicate and fire an event ships in a later release
# (step 8 lands before launch). Rejecting event rules now would break
# manifest workflows that already declare them; instead `stream_topics`/
# `stop_streams` report them honestly via `events_configured`/
# `events_active` (see each function's Returns) and log this once whenever
# a call leaves any event rule configured.
EVENTS_NOT_ACTIVE_MSG = (
    "event rules are stored and forwarded but not evaluated until the event runtime ships"
)


def _warn_if_events_configured(events: list) -> None:
    if events:
        logging.getLogger(__name__).warning(EVENTS_NOT_ACTIVE_MSG)


def _fleet_installed() -> bool:
    """Whether the optional `fleet` dependency group (paho-mqtt) is importable.

    Uses `find_spec`, not an actual import: `fleet_status()` must answer
    without ever eagerly importing paho (unlike `require_fleet()`, which
    does import it, for the operations that actually need it running).

    `find_spec` on a dotted name ("paho.mqtt") first resolves the PARENT
    package ("paho") to read its `__path__`. If `paho` is present on the
    path only as a half-installed namespace package -- no `__init__.py`, or
    otherwise broken -- that parent-resolution step can itself raise
    `ModuleNotFoundError` or `ValueError` instead of `find_spec` cleanly
    returning `None` (Codex round 3, P2). `fleet_status()`'s whole contract
    is "never raises", so either is treated as "not installed" here rather
    than propagating.
    """
    try:
        return importlib.util.find_spec("paho.mqtt") is not None
    except (ModuleNotFoundError, ValueError):
        return False


def _service_status(service: FleetService | None) -> dict | None:
    """`service.status()`, guarded: `fleet_status()` must never raise (Codex round 3, P2).

    `status()` reads live spool stats (`Spool.stats()` rescans a lane's
    active segment on first access), so a corrupt segment on disk can raise
    `SpoolCorruptError` straight out of a call that is supposed to be a
    read-only diagnostic. Surface the failure IN the report instead of
    letting it blow up the report itself.
    """
    if service is None:
        return None
    try:
        return service.status()
    except Exception as exc:  # fleet_status() must never raise, by design (Codex round 3)
        return {"error": f"{type(exc).__name__}: {exc}"}


def fleet_status() -> dict:
    """Report this robot's fleet-streaming state -- the local source of truth (spec §7).

    Calls NO gate: this must answer sanely in every state (uninstalled,
    disabled, unenrolled, no service running), not raise when one of those
    is exactly what the caller is trying to diagnose.

    Returns:
        ```
        {
          "enabled": bool,        # settings.FLEET_ENABLED
          "installed": bool,      # the `fleet` dependency group (paho-mqtt) is importable
          "enrolled": bool,       # a complete identity is stored on disk
          "identity": {"tenant", "robot_id", "broker_url", "cert_expires_at",
                       "renew_url"} | None,  # never key material, never paths
          "service": "running" | "paused" | "stopped",
          "channels": list[dict],  # resolved channel descriptors, [] if no service
          "status": dict | None,   # FleetService.status()'s §4 counters block, verbatim,
                                    # {"error": "<class>: <msg>"} if status() itself raised
                                    # (e.g. a corrupt spool segment -- Codex round 3, P2),
                                    # None if no service is running
        }
        ```

    """
    service = startup.fleet_service()
    if service is None:
        service_state = "stopped"
    elif service.paused:
        service_state = "paused"
    else:
        service_state = "running"

    # Single load, not `is_enrolled()` followed by a separate `load_identity()`
    # call -- that two-call shape has a TOCTOU: a corrupt/deleted identity
    # between the two calls would raise `FleetNotEnrolledError` through this
    # function's never-raises contract. One `load_identity()` call, caught
    # here, closes that window structurally.
    try:
        loaded = identity.load_identity(settings.FLEET_IDENTITY_DIRECTORY)
    except FleetNotEnrolledError:
        loaded = None

    identity_summary = None
    if loaded is not None:
        identity_summary = {
            "tenant": loaded.tenant,
            "robot_id": loaded.robot_id,
            "broker_url": loaded.broker_url,
            "cert_expires_at": loaded.expires_at,
            "renew_url": loaded.renew_url,
        }

    return {
        "enabled": bool(settings.FLEET_ENABLED),
        "installed": _fleet_installed(),
        "enrolled": loaded is not None,
        "identity": identity_summary,
        "service": service_state,
        "channels": service.channels if service is not None else [],
        "status": _service_status(service),
    }


def pause_streaming(discard: bool = False) -> dict:
    """Pause the live `FleetService`, if any: go offline, keep identity + rules.

    `require_fleet()` first -- `FLEET_ENABLED=0` raises `FleetDisabledError`
    before the holder is touched at all.

    Args:
        discard: When true, additionally empties the channels spool lane's
            still-unacked backlog (events/heartbeat are never-drop lanes and
            are left untouched -- see `FleetService.pause`).

    Returns:
        `{"service": "stopped", "changed": False}` when no service is
        running (an idempotent no-op). Otherwise `{"service": "paused",
        "changed": bool, "discarded": discard}`, where `changed` is `True`
        only when this call actually transitioned a running service to
        paused (a second `pause_streaming()` call on an already-paused
        service reports `changed: False`).

    Raises:
        FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.

    """
    require_fleet()
    with _control_lock:
        service = startup.fleet_service()
        if service is None:
            return {"service": "stopped", "changed": False}
        changed = not service.paused
        service.pause(discard=discard)
        return {"service": "paused", "changed": changed, "discarded": discard}


def resume_streaming() -> dict:
    """Resume the live `FleetService`, if any: mirror image of `pause_streaming`.

    `require_fleet()` first -- `FLEET_ENABLED=0` raises `FleetDisabledError`
    before the holder is touched at all.

    Returns:
        `{"service": "stopped", "changed": False}` when no service is
        running (an idempotent no-op). Otherwise `{"service": "running",
        "changed": bool}`, where `changed` is `True` only when this call
        actually transitioned a paused service back to running (calling
        this on an already-running service reports `changed: False`).

    Raises:
        FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.

    """
    require_fleet()
    with _control_lock:
        service = startup.fleet_service()
        if service is None:
            return {"service": "stopped", "changed": False}
        changed = service.paused
        service.resume()
        return {"service": "running", "changed": changed}


def enroll_identity(token: str, enroll_url: str) -> dict:
    """Enroll this robot with the fleet cloud: keygen, CSR, store identity (spec §6).

    `require_fleet()` first. Already enrolled -> `EnrollmentError(0, ...)`
    (ruling: status 0 means "no server was ever contacted", consistent with
    that code's existing transport-failure meaning) rather than silently
    re-enrolling over an existing identity.

    Returns:
        `{"tenant", "robot_id", "broker_url", "expires_at"}` -- NEVER the
        token, and never key material or paths.

    Raises:
        FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.
        EnrollmentError: already enrolled, or any of `identity.enroll()`'s
            own failure modes (see its docstring).

    """
    require_fleet()
    with _control_lock:
        directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
        if identity.is_enrolled(directory):
            raise EnrollmentError(0, "already enrolled; run unenroll_fleet_identity first")
        enrolled = identity.enroll(token, enroll_url, directory)
        return {
            "tenant": enrolled.tenant,
            "robot_id": enrolled.robot_id,
            "broker_url": enrolled.broker_url,
            "expires_at": enrolled.expires_at,
        }


def unenroll_identity() -> dict:
    """Unenroll this robot: stop any live service, delete identity, clear manifest streams.

    NO gate (ruling): unenrolling only makes the fleet subsystem MORE
    inert, so it must work even with `FLEET_ENABLED=0` or paho not
    installed.

    Best-effort stops the live `FleetService`, if any, and clears the
    holder regardless of whether the stop itself succeeded.
    `identity.delete_identity()` is the ONLY deletion path (ruling a) --
    nothing here unlinks a file directly. The manifest's `streams:` section,
    if persisted, is removed too (`subscriptions:` and everything else in
    the manifest is left untouched). Idempotent: a second call finds
    nothing enrolled and returns `deleted: []` without error.

    Returns:
        `{"deleted": list[str], "streams_removed": bool, "service": "stopped"}`.

    """
    with _control_lock:
        service = startup.fleet_service()
        if service is not None:
            try:
                service.stop()
            except Exception:
                logging.warning("Best-effort stop of the live FleetService during unenroll failed")
            finally:
                startup.set_fleet_service(None)
        deleted = identity.delete_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        streams_removed = _persist_streams(None)
        return {"deleted": deleted, "streams_removed": streams_removed, "service": "stopped"}


def stream_topics(channels: list[dict] | None, events: list[dict] | None) -> dict:
    """Add/replace channel and event rules at runtime, restart, persist (spec §7).

    `require_fleet()` first. Every entry is validated (`ChannelRule.build`/
    `EventRule.build`, typed `StreamConfigError`) BEFORE any state changes.
    The validated rules are merged onto the current config (the running
    service's `.streams`, else the manifest's persisted streams, else an
    empty `StreamsConfig`) per the binding merge ruling: a channel rule
    REPLACES any existing rule for the same `topic`; an event rule replaces
    by `name`. The merged config becomes the new live service (via
    `_restart_service`) and is persisted back to the manifest, if one is
    configured.

    Returns:
        `{"service": "running" | "paused", "channels": list[dict],
        "events_configured": list[str], "events_active": False, "persisted":
        bool}`, plus `"persist_error": str` ONLY when persisting after an
        otherwise-successful restart failed (see below). `"paused"` when the
        service this replaced was paused: `_restart_service` preserves
        paused-ness across the rule-change restart (I1 ruling) -- a brief
        reconnect blip to republish the schema, then straight back to
        paused. `events_configured` lists every event rule name now stored
        and forwarded; `events_active` is always `False` -- event rules are
        accepted, merged, and persisted, but nothing on-robot evaluates them
        yet (honest-reporting ruling, Codex round 3: the event runtime ships
        in a later release). A WARNING is logged once whenever
        `events_configured` is non-empty.

        Live-vs-persisted atomicity (Codex round 3, P2; widened P2 follow-up
        on PR #214 to also cover `OSError`): the restart above has ALREADY
        happened by the time persistence is attempted, so a persist failure
        here (an unparsable manifest, or an `OSError` from a read-only
        manifest directory, a full disk, etc) does NOT raise -- raising
        would misreport a live change that genuinely succeeded as a total
        failure. Instead `persisted` is `False` and `persist_error` carries
        the problem, so the caller sees the true state: the robot IS running
        the new rules; the manifest just doesn't reflect it yet.

    Raises:
        FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.
        StreamConfigError: an invalid rule dict, or `_restart_service`'s own
            failure modes (no covering sink for the merged topics, no
            viable broker, etc).
        FleetNotEnrolledError: `_restart_service`'s `resolve_publisher_kwargs`
            call -- no broker configured and this robot isn't enrolled.

        Failure-outcome contract (see `_restart_service`'s docstring): every
        one of the above is checked BEFORE the old service (if any) is
        touched, so a raise here always leaves a previously-running service
        running, completely untouched. The only way this call can leave the
        holder `None` on a raise is a failure INSIDE the new
        `FleetService.start()` itself, which necessarily runs after the old
        service has already been stopped.

    """
    require_fleet()
    with _control_lock:
        new_channels = [
            config.ChannelRule.build(entry, label=f"channels[{i}]")
            for i, entry in enumerate(channels or [])
        ]
        new_events = [
            config.EventRule.build(entry, label=f"events[{i}]")
            for i, entry in enumerate(events or [])
        ]

        current = _current_streams()
        merged = config.StreamsConfig(
            broker=current.broker,
            flush_interval_s=current.flush_interval_s,
            channels=_merge_by_key(current.channels, new_channels, key=lambda rule: rule.topic),
            events=_merge_by_key(current.events, new_events, key=lambda rule: rule.name),
        )

        _restart_service(merged)
        persisted, persist_error = _persist_or_report(merged.to_manifest())
        service = startup.fleet_service()
        event_names = [rule.name for rule in merged.events]
        _warn_if_events_configured(event_names)
        result = {
            "service": "paused" if service.paused else "running",
            "channels": service.channels,
            "events_configured": event_names,
            "events_active": False,
            "persisted": persisted,
        }
        if persist_error is not None:
            result["persist_error"] = persist_error
        return result


def stop_streams(channels: list[str] | None, events: list[str] | None) -> dict:
    """Remove channel/event rules by name at runtime, restart, persist (spec §7).

    `require_fleet()` first. `channels` names resolved (or renamed) channel
    names -- see `config.channel_name` -- not field paths or topics; a
    matched field is dropped from its rule, a rule left with no fields (or a
    matched geo rule) is dropped entirely. `events` names event rule
    `name`s. Unknown names are no-ops (idempotency ruling).

    When nothing actually changed, a running service is left completely
    alone (no restart -- the SAME service object stays in the holder); when
    something did change, `_restart_service` rebuilds it -- even down to an
    empty config (heartbeat = liveness; full offline is `pause_streaming`/
    `unenroll_identity`). When no service is running at all, this never
    starts one (that is `stream_topics`'s job) -- it only updates the
    persisted manifest.

    A no-op (nothing matched) never touches the manifest either -- not just
    "no restart": `_persist_streams` is skipped entirely, so a manifest that
    had no `streams:` section stays exactly as it was (byte-identical), and
    `persisted` reports `False`.

    Returns:
        `{"service": "running" | "paused" | "stopped", "channels":
        list[dict], "events_configured": list[str], "events_active": False,
        "changed": bool, "persisted": bool}`, plus `"persist_error": str`
        ONLY when persisting after an actual, RESTARTED (`changed` AND a
        live service) change failed (see below). `"paused"` when a restart
        happened and the service it replaced was paused: `_restart_service`
        preserves paused-ness across the restart (I1 ruling) -- a brief
        reconnect blip to republish the schema, then straight back to
        paused. `events_configured` lists the event rule names still stored
        and forwarded after this call; `events_active` is always `False` --
        nothing on-robot evaluates them yet (honest-reporting ruling, Codex
        round 3: the event runtime ships in a later release). A WARNING is
        logged once whenever `events_configured` is non-empty.

        Live-vs-persisted atomicity (Codex round 3, P2; widened P2 follow-up
        on PR #214 to also cover `OSError`) -- ONLY for the restarted case:
        when `changed` is True AND a live service was running, the restart
        has ALREADY happened (and, per `_restart_service`'s own
        failure-outcome contract, already SUCCEEDED) by the time
        persistence is attempted, so a persist failure there (an unparsable
        manifest, or an `OSError` from a read-only manifest directory, a
        full disk, etc) does NOT raise; `persisted` is `False` and
        `persist_error` carries the problem instead, so a genuinely
        successful live change is never misreported as a failure.

        Persist-ONLY failure-outcome contract (Codex round 3 follow-up, PR
        #214 P2 on comment 3924387659): when `changed` is True but NO live
        service was running, this call's ENTIRE effect is the manifest
        write -- there is no successful restart for a swallowed persist
        failure to protect. A persist failure there PROPAGATES typed
        (`StreamConfigError` or `OSError`) instead of being folded into
        `persist_error`: the call did nothing, so it says so, rather than
        reporting a misleadingly benign-looking `persisted: False` for what
        is actually a total failure.

    Raises:
        FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.
        FleetNotEnrolledError | StreamConfigError: via `_restart_service`
            (no covering sink, no viable broker, etc) -- only reachable when
            something changed and a service is running; per its failure-
            outcome contract, such a failure leaves the OLD service running
            untouched (see `_restart_service`'s docstring).
        StreamConfigError | OSError: from `_persist_streams` itself, ONLY
            when `changed` is True and NO live service was running (the
            persist-only path above) -- an unparsable manifest, or a
            read-only manifest directory / full disk / other write failure.

    """
    require_fleet()
    with _control_lock:
        channel_names = set(channels or [])
        event_names = set(events or [])
        current = _current_streams()

        remaining_channels, channels_changed = _drop_channel_names(current.channels, channel_names)
        remaining_events = [rule for rule in current.events if rule.name not in event_names]
        events_changed = len(remaining_events) != len(current.events)
        changed = channels_changed or events_changed

        remaining = config.StreamsConfig(
            broker=current.broker,
            flush_interval_s=current.flush_interval_s,
            channels=remaining_channels,
            events=remaining_events,
        )

        service = startup.fleet_service()
        if changed and service is not None:
            _restart_service(remaining)
            service = startup.fleet_service()

        # A pure no-op must not touch the manifest at all (not even a rewrite
        # with identical content) -- a manifest with no `streams:` section
        # stays byte-identical, and `persisted` truthfully reports `False`.
        persist_error = None
        if changed and service is not None:
            # A restart already happened here (and, per `_restart_service`'s
            # own failure-outcome contract, already SUCCEEDED -- a failing
            # restart would have raised before this line was ever reached).
            # `_persist_or_report`'s swallow-and-report ruling exists
            # precisely to avoid masking that success (Codex round 3, P2,
            # widened in the PR #214 follow-up).
            persisted, persist_error = _persist_or_report(remaining.to_manifest())
        elif changed:
            # No live service, so nothing was restarted -- the manifest
            # write IS this call's entire effect (Codex round 3 follow-up,
            # PR #214 P2 on comment 3924387659). There is no successful
            # restart here for a swallowed failure to protect, so a persist
            # failure propagates typed instead: the call did nothing, and
            # says so, rather than reporting a misleadingly benign-looking
            # `persisted: False` for what is actually a total failure.
            persisted = _persist_streams(remaining.to_manifest())
        else:
            persisted = False
        if service is None:
            service_state = "stopped"
        elif service.paused:
            service_state = "paused"
        else:
            service_state = "running"
        remaining_event_names = [rule.name for rule in remaining.events]
        _warn_if_events_configured(remaining_event_names)
        result = {
            "service": service_state,
            "channels": service.channels if service is not None else [],
            "events_configured": remaining_event_names,
            "events_active": False,
            "changed": changed,
            "persisted": persisted,
        }
        if persist_error is not None:
            result["persist_error"] = persist_error
        return result


def _merge_by_key(current: list, new: list, *, key: Callable[[object], object]) -> list:
    """Merge `new` rules onto `current` by `key`: a matching key REPLACES, else appends."""
    merged: dict = {key(rule): rule for rule in current}
    for rule in new:
        merged[key(rule)] = rule
    return list(merged.values())


def _drop_channel_names(
    current: list[config.ChannelRule], names: set[str]
) -> tuple[list[config.ChannelRule], bool]:
    """Drop every field (or whole geo rule) whose resolved `config.channel_name` is in `names`.

    A `fields:` rule loses just the matched fields (and their now-orphaned
    `renames` entries -- an unpruned rename key would fail `_check_renames`
    on the next `resolve()`); it is dropped entirely once no fields remain.
    A `geo:` rule is all-or-nothing: matched -> dropped, else untouched.

    Returns:
        (remaining rules, whether anything actually changed).

    """
    remaining: list[config.ChannelRule] = []
    changed = False
    for rule in current:
        if rule.fields is not None:
            kept = [f for f in rule.fields if config.channel_name(rule, f) not in names]
            if not kept:
                changed = True
                continue
            if kept == rule.fields:
                remaining.append(rule)
            else:
                changed = True
                remaining.append(
                    rule.model_copy(
                        update={
                            "fields": kept,
                            "renames": {k: v for k, v in rule.renames.items() if k in kept},
                        }
                    )
                )
        else:
            if config.channel_name(rule, "geo") in names:
                changed = True
                continue
            remaining.append(rule)
    return remaining, changed


def _current_streams() -> config.StreamsConfig:
    """Return the config to merge/drop rules against: running service, else manifest, else empty."""
    service = startup.fleet_service()
    if service is not None:
        return service.streams
    loaded = config.load_streams(_read_manifest_doc())
    return loaded if loaded is not None else config.StreamsConfig()


def _no_covering_sink_error(source_topics: set[str]) -> StreamConfigError:
    return StreamConfigError(
        "streams",
        "all streams: source topics "
        f"{sorted(source_topics)} must be subscribed within a SINGLE "
        "startup manifest 'subscriptions:' entry -- fleet streaming "
        "(v1) cannot span multiple subscription entries' sinks; list "
        "every one of these topics under one entry's 'topics:'",
    )


def _resolve_sink(streams: config.StreamsConfig, old: FleetService | None) -> object:
    """Pick the sink `_restart_service` will (re)build against -- raises before anything else.

    When a service is already running, its sink is reused ONLY if it still
    covers every one of `streams`'s source topics -- checked HERE, not left
    to `FleetService.start()`'s own `StreamsConfig.resolve()`, precisely so
    an uncovered/typo'd topic raises before `old` is ever touched (Codex
    review: the previous version deferred this check until after `old` had
    already been stopped and the holder cleared, so a bad rule call
    destroyed a working service instead of being rejected cleanly). With no
    service running, the first of `base.live_sinks()` whose
    `subscribed_topics` cover every source topic is used instead (reusing
    `startup._fleet_source_topics`/`_find_covering_sink`).
    """
    source_topics = startup._fleet_source_topics(streams)
    if old is not None:
        if not source_topics <= set(old.sink.subscribed_topics):
            raise _no_covering_sink_error(source_topics)
        return old.sink
    candidates = [(s, s.subscribed_topics) for s in base.live_sinks()]
    sink = startup._find_covering_sink(source_topics, candidates)
    if sink is None:
        raise _no_covering_sink_error(source_topics)
    return sink


def _load_identity_or_none() -> identity.Identity | None:
    """Single-load `identity.load_identity`, `None` on `FleetNotEnrolledError`.

    NOT `is_enrolled()` followed by a separate `load_identity()` call --
    that two-call shape has the same TOCTOU `fleet_status()` was fixed for
    (Task 4's carried finding): a corrupt/deleted identity between the two
    calls would raise here instead of degrading to "unenrolled".
    """
    try:
        return identity.load_identity(settings.FLEET_IDENTITY_DIRECTORY)
    except FleetNotEnrolledError:
        return None


def _restart_service(streams: config.StreamsConfig) -> None:
    """Rebuild path for `stream_topics`/`stop_streams` (mirrors `startup._start_fleet`).

    Failure-outcome contract: every check that can be done WITHOUT touching
    the old service -- sink coverage (`_resolve_sink`), identity resolution
    (`_load_identity_or_none`), broker/auth resolution
    (`resolve_publisher_kwargs`), and constructing the new publisher/spool/
    `FleetService` -- runs first, while the old service (if any) is still
    fully intact and untouched. Only once ALL of that has succeeded is the
    old service stopped and the holder cleared. So: a validation failure
    (bad topic, no viable broker, etc) always leaves the OLD service running
    exactly as it was; the only way this call can leave the holder `None` is
    a failure INSIDE the new `FleetService.start()` itself, which runs after
    the old service has already been torn down (there is no way to
    interleave "start the new one" before "stop the old one" -- they would
    otherwise both be tapping the same sink's buffers at once).

    Any failure here propagates typed -- unlike `startup._start_fleet`
    (a boot-time path that swallows everything into a report), these are
    interactive tools: a caller needs to see exactly why a restart failed.

    Paused-ness is preserved across the restart (I1 ruling): `old.paused` is
    captured BEFORE `old` is touched at all, and if it was paused, the new
    service is paused again immediately after `service.start()`. A rule
    change on a paused service must not silently bring it back online --
    the brief reconnect (to republish the retained schema) is an accepted,
    documented blip, not a resume.
    """
    old = startup.fleet_service()
    was_paused = old.paused if old is not None else False
    sink = _resolve_sink(streams, old)

    identity_obj = _load_identity_or_none()
    publisher_kwargs = resolve_publisher_kwargs(streams, identity_obj)
    publisher = MqttPublisher(**publisher_kwargs)
    spool = Spool.for_robot(identity_obj.robot if identity_obj is not None else "dev/robot")
    service = FleetService(
        sink=sink, streams=streams, publisher=publisher, spool=spool, identity=identity_obj
    )

    if old is not None:
        try:
            old.stop()
        except Exception:
            logging.warning("Failed to stop the previous FleetService before replacing it")
        finally:
            startup.set_fleet_service(None)

    service.start()
    if was_paused:
        service.pause()
    startup.set_fleet_service(service)


def _manifest_path() -> pathlib.Path | None:
    raw = settings.STARTUP_PIPELINES_FILE
    return pathlib.Path(raw) if raw else None


def _read_manifest_doc() -> dict:
    """Read the startup manifest into a dict; `{}` when unset, unwritten, or empty.

    Raises:
        StreamConfigError: the file exists but fails to parse as YAML,
            fails to even decode as UTF-8 (`Path.read_text()` raises
            `UnicodeDecodeError` on invalid bytes -- before `yaml.safe_load`
            ever runs, so this must be caught alongside `yaml.YAMLError`,
            not just it; Codex round 3 follow-up, PR #214 P2, comment
            3927023413's sibling finding), or parses to something other
            than a mapping -- so a corrupt manifest is never silently
            treated as empty and then clobbered by a subsequent
            `_persist_streams` write.

    """
    path = _manifest_path()
    if path is None or not path.is_file():
        return {}
    try:
        doc = yaml.safe_load(path.read_text())
    except (yaml.YAMLError, UnicodeDecodeError) as exc:
        raise StreamConfigError("manifest", f"unparsable manifest at {path}: {exc}") from exc
    if doc is None:
        return {}
    if not isinstance(doc, dict):
        raise StreamConfigError("manifest", f"manifest at {path} must be a mapping, got {doc!r}")
    return doc


def _persist_streams(streams_manifest: dict | None) -> bool:
    """Set (or, when `None`, remove) the manifest's `streams:` section; return whether written.

    `settings.STARTUP_PIPELINES_FILE` unset -> `False`, no error: the
    in-memory control-plane change is still fully in effect, there's just
    no manifest file to persist it to. Otherwise reads the current manifest
    (`_read_manifest_doc` -- missing -> `{}`, unparsable -> raises rather
    than clobbering a human-maintained file), updates just the `"streams"`
    key, and writes the whole document back atomically (sibling tempfile +
    `os.replace`) so a crash mid-write never corrupts it. When the manifest
    file already exists, its permission mode is copied onto the tempfile
    before the replace -- `tempfile.mkstemp` creates at 0600 by default, so
    without this a manifest a human left group/other-readable (or made
    read-only) would silently end up 0600 after its first control-plane
    write. A freshly-created manifest (no prior file) keeps that 0600
    default -- there is no prior mode to preserve, and this file can carry
    fleet broker configuration.
    """
    path = _manifest_path()
    if path is None:
        return False
    preexisting = path.is_file()
    mode = stat.S_IMODE(path.stat().st_mode) if preexisting else None
    doc = _read_manifest_doc()
    if streams_manifest is None:
        doc.pop("streams", None)
    else:
        doc["streams"] = streams_manifest
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = pathlib.Path(tmp_name)
    try:
        if mode is not None:
            os.fchmod(fd, mode)
        with os.fdopen(fd, "w") as handle:
            yaml.safe_dump(doc, handle, sort_keys=False)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return True


def _persist_or_report(streams_manifest: dict | None) -> tuple[bool, str | None]:
    """`_persist_streams`, guarded so persist failure never masks an already-successful live change.

    By the time `stream_topics`/`stop_streams` call this, `_restart_service`
    has ALREADY succeeded -- the robot is really running the new rules. If
    persisting that to disk fails, letting the exception propagate here
    would surface as a raised exception from `stream_topics`/`stop_streams`,
    which reads to a caller as "nothing happened" when actually the live
    change DID happen and only the manifest write failed. So: catch it here,
    and let the caller report `persisted=False` plus the error message
    alongside its otherwise-successful result, instead of raising
    post-restart.

    Two failure modes are caught (Codex round 3, PR #214 P2 follow-up):
    `StreamConfigError` (`_read_manifest_doc` refusing to clobber an
    unparsable manifest it can't safely read first) and `OSError` (a
    read-only manifest directory, a full disk, or any other failure from
    `_persist_streams`'s `mkdir`/`mkstemp`/`fchmod`/write/`os.replace`
    calls) -- both are equally "the live change succeeded, only the disk
    write failed", so both get the same non-raising treatment.

    Returns:
        `(persisted, persist_error)`: `persist_error` is `None` on success
        (including the "no manifest configured" `False` case, which is not
        an error).

    """
    try:
        return _persist_streams(streams_manifest), None
    except (StreamConfigError, OSError) as exc:
        return False, str(exc)
