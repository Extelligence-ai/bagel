"""Fleet conformance selftest: publish a known sequence with no robot (spec §8).

This is the "fleet selftest" helper spec §8 asks for: a fixed, deterministic
channel/heartbeat/event sequence that a fleet service can subscribe to and
validate against `doc/fleet_protocol_v1.md` without a real robot in the loop.
`run_selftest` is the testable core (no CLI, no gate) -- `main` wires it to
the same production connection policy (`connect.resolve_publisher_kwargs`)
every other fleet entry point uses, so a selftest run proves the SAME
dev-insecure/mTLS rules a real robot would hit, not a shortcut around them.

Run this with fleet streaming paused or stopped on the target robot/dev rig:
a concurrently running `FleetService`/`StreamRouter` writes to the same real
spool lanes this selftest uses (`Spool.for_robot`). This is no longer
required for correctness (Codex round 3): the whole run holds
`spool.exclusive()` (P1b) so a concurrent writer simply waits its turn
instead of racing for seqs, and every allocate-then-write call goes through
`Spool.append_next()` (P1 follow-up, comment 3924082774) so allocation and
write can never be interleaved by a DIFFERENT writer even without the lock.
Pausing first remains good practice -- it avoids the wait -- see the runbook.

Invocation is `uv run python -m src.sink.publish.selftest` -- there is no
console-script entry point. This repo ships as a Docker image, not a PyPI
package (see AGENTS.md), so a `pyproject.toml` `[project.scripts]` entry
would be dead weight nothing installs.
"""

import argparse
import json
import pathlib
import sys
import time
import uuid
from collections.abc import Callable

from settings import settings
from src.sink.publish import (
    FleetDisabledError,
    FleetNotEnrolledError,
    FleetNotInstalledError,
    StreamConfigError,
    require_fleet,
)
from src.sink.publish.config import StreamsConfig
from src.sink.publish.connect import resolve_publisher_kwargs
from src.sink.publish.heartbeat import build_heartbeat, disk_free
from src.sink.publish.identity import Identity, load_identity
from src.sink.publish.mqtt import MqttPublisher
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.spool import Spool, SpoolLockedError

DEFAULT_BATCHES = 10
DEFAULT_INTERVAL_S = 0.5
DEFAULT_LOCK_TIMEOUT_S = 5.0


class SelftestPreconditionError(ValueError):
    """Raised when `run_selftest` refuses to start (spec §8 conformance ruling).

    A `ValueError` subclass so it's caught by every existing spool-`ValueError`
    handler (including this module's own `main()`) without needing a special
    case. Currently the only precondition: a spool lane with pending unacked
    backlog (see `_check_no_pending_backlog`).
    """


# A fixed, documented four-channel schema covering every §3 wire type
# (number, bool, string, geo). Never resolved from a real topic/manifest --
# this IS the conformance fixture, so it must never drift with a real robot's
# schema.
SELFTEST_CHANNELS = [
    {
        "c": "selftest.number",
        "type": "number",
        "unit": "1",
        "source_topic": "selftest",
        "source_field": "number",
    },
    {
        "c": "selftest.bool",
        "type": "bool",
        "unit": None,
        "source_topic": "selftest",
        "source_field": "bool",
    },
    {
        "c": "selftest.string",
        "type": "string",
        "unit": None,
        "source_topic": "selftest",
        "source_field": "string",
    },
    {
        "c": "selftest.geo",
        "type": "geo",
        "unit": None,
        "source_topic": "selftest",
        "source_field": "lat=lat,lon=lon",
    },
]


def _build_batch(i: int, t: float) -> dict:
    """One deterministic batch's samples for iteration `i` at wall-clock `t`."""
    return {
        "v": 1,
        "t_batch": t,
        "samples": [
            {"c": "selftest.number", "t": t, "v": float(i)},
            {"c": "selftest.bool", "t": t, "v": i % 2 == 0},
            {"c": "selftest.string", "t": t, "v": f"selftest-{i}"},
            {
                "c": "selftest.geo",
                "t": t,
                "v": {"lat": 52.0 + i * 0.001, "lon": 13.0 + i * 0.001},
            },
        ],
    }


def _stamp_seq(payload: dict, seq: int) -> dict:
    """Stamp `payload["seq"] = seq` in place and return it.

    A small `Spool.append_next()` `build` helper for the common case: the
    wire payload dict already exists (e.g. `_build_batch`'s output) and just
    needs the atomically-allocated seq written into its own embedded `seq`
    field before it's spooled -- MUTATING in place (not returning a copy)
    matters here, since the caller keeps its own reference to the same dict
    to publish afterward, and both must see the identical stamped seq.
    """
    payload["seq"] = seq
    return payload


def _check_no_pending_backlog(spool: Spool) -> None:
    """Refuse to run against a spool lane with pending unacked entries (C1 ruling).

    `run_selftest` allocates real-lane seqs and acks them as it goes
    (advance-to semantics -- see `Spool.ack`). If either lane already has
    pending backlog -- e.g. a paused service's queued-but-unsent data --
    the FIRST ack this run issues would advance that lane's watermark past
    the pending backlog, silently dropping it, even though this run never
    touched those specific records. Checked before any connect/append side
    effect (see `run_selftest`'s call site), so a refusal here leaves the
    spool completely untouched.
    """
    for lane in ("channels", "events"):
        if next(spool.pending(lane), None) is not None:
            raise SelftestPreconditionError(
                f"selftest refused: spool lane '{lane}' has pending unacked entries. "
                "Running the selftest now would advance this lane's watermark past "
                "that backlog and silently drop it. Let the service drain it, or "
                "discard it first via pause_fleet_streaming(discard=True)."
            )


DEFAULT_LIVE_SESSION_PROBE_TIMEOUT_S = 1.5


def _check_no_live_session(
    publisher: Publisher, *, timeout_s: float = DEFAULT_LIVE_SESSION_PROBE_TIMEOUT_S
) -> None:
    """Refuse to run while the robot's live fleet session is connected.

    Codex round 3 P1, comment 3927287968.

    Investigation for the earlier rounds this round follows up on
    (`client_id_suffix`, `retain_messages=False`) established that neither
    fix protects a CONNECTED live subscriber: the selftest's fixture schema
    still reaches a live ingestor as a schema update the instant it's
    published, remapping live channel batches to the fixture's four
    `selftest.*` channels until the live service's next reconnect --
    non-retention only protects a LATE subscriber, and a distinct client id
    only stops the broker from displacing the live session, not from
    delivering this session's publishes to it. The only structural fix is
    to never publish at all while a live session exists.

    Detection uses the wire itself, since nothing else (spool state,
    process state) tells this process whether some OTHER process/robot
    already holds a live session on this broker: subscribes to the robot's
    own heartbeat topic (`Publisher.wait_for_retained_heartbeat`, if the
    publisher offers it -- see `MqttPublisher`'s implementation) and waits a
    short bounded window for a RETAINED message. A live service keeps its
    heartbeat retained with `online: true` for as long as it's connected; a
    paused/stopped service (or an unclean disconnect's last-will) retains
    `online: false`; a fresh robot/broker has nothing retained at all.
    Refuses ONLY on the first case.

    Called AFTER `publisher.connect()` (the probe needs a live connection to
    subscribe) but BEFORE any publish (see `run_selftest`'s call site) --
    a refusal here leaves nothing published. Combined with
    `retain_messages=False` and no last-will (round 8's fix), even the
    aborted connection attempt leaves no RETAINED residue on the broker.
    The connection itself is torn down silently (`Publisher.
    disconnect_without_publishing`, if offered -- see `MqttPublisher`'s
    implementation) before raising, releasing this session's deterministic
    client id promptly rather than leaving it to eventually be
    garbage-collected -- deliberately NOT `publisher.close()`, which would
    publish a non-retained clean-stop beat that could itself deliver a
    misleading momentary `online: false` to whoever is watching the live
    session's own heartbeat topic.

    If `publisher` doesn't offer `wait_for_retained_heartbeat` (most test
    doubles, and any future non-MQTT `Publisher` implementation), this is a
    no-op: the check simply can't be performed, and `run_selftest` proceeds
    as it always did before this round -- see `wait_for_retained_heartbeat`'s
    own docstring for why this isn't grown into the `Publisher` ABC instead.
    """
    probe = getattr(publisher, "wait_for_retained_heartbeat", None)
    if probe is None:
        return
    beat = probe(timeout_s=timeout_s)
    if beat is not None and beat.get("online") is True:
        disconnect_silently = getattr(publisher, "disconnect_without_publishing", None)
        if disconnect_silently is not None:
            disconnect_silently()
        raise SelftestPreconditionError(
            "selftest refused: a live fleet session is connected for this "
            "robot; pause or stop the fleet service first"
        )


def _open_live_session_watch(publisher: Publisher) -> object | None:
    """Arm the ONGOING live-session watch for the rest of the run, if offered.

    Codex round 3 follow-up (PR #214 P1, comment 3927287968's own
    follow-up): `_check_no_live_session` only ever proves the heartbeat
    topic's state at the moment it's called -- right after `connect()`,
    before the schema publish. A live `FleetService` that RESUMES any time
    AFTER that check (mid-run, between batches, or even between the last
    batch and the heartbeat/event publishes) reopens the exact schema-
    pollution window `_check_no_live_session` exists to close, since
    nothing about a one-shot START check tells `run_selftest` about a
    session that connects a moment later.

    `publisher.watch_live_session()` (see `MqttPublisher`'s implementation),
    called here right after `_check_no_live_session` has cleared the START
    state, keeps the heartbeat subscription open for the remainder of the
    run -- `run_selftest` polls the returned watch's `.detected` Event
    between batches and before the heartbeat/event publishes via
    `_abort_if_live_session_detected`. Same optional-capability pattern as
    `_check_no_live_session`'s own probe (`getattr(...,  None)`): a
    `Publisher` that doesn't offer this (most test doubles, and any future
    non-MQTT implementation) simply isn't watched, matching this module's
    existing behavior before this round.
    """
    open_watch = getattr(publisher, "watch_live_session", None)
    if open_watch is None:
        return None
    return open_watch()


def _abort_if_live_session_detected(watch: object | None, publisher: Publisher) -> None:
    """Raise the same typed refusal as `_check_no_live_session` if `watch` fired.

    Called between every channel batch and immediately before the
    heartbeat and event publishes (see `run_selftest`) -- checking the
    SAME `watch.detected` Event a live beat's paho callback sets the
    instant it arrives (Codex round 3 follow-up, PR #214 P1, comment
    3927287968's own follow-up), so a live service resuming at any point
    during the run is caught before this run's next publish, not just at
    the very start.

    Mirrors `_check_no_live_session`'s own refusal exactly: the connection
    is torn down silently via `disconnect_without_publishing` (never
    `close()`, which would publish a clean-stop beat and could itself
    mislead whoever is watching the live session's own heartbeat topic) --
    the same typed `SelftestPreconditionError`, the same "exit 1" contract
    via `main()`'s `_EXPECTED_ERRORS`. `watch.stop()` is deliberately NOT
    called here: `disconnect_without_publishing` already tears down the
    whole underlying connection the watch's subscription lives on, so
    there is nothing left for `.stop()` to usefully unwind.
    """
    if watch is None or not watch.detected.is_set():  # type: ignore[attr-defined]
        return
    disconnect_silently = getattr(publisher, "disconnect_without_publishing", None)
    if disconnect_silently is not None:
        disconnect_silently()
    raise SelftestPreconditionError(
        "selftest refused: a live fleet session connected for this robot "
        "partway through this run; pause or stop the fleet service first"
    )


def run_selftest(  # noqa: PLR0913
    publisher: Publisher,
    spool: Spool,
    *,
    batches: int = DEFAULT_BATCHES,
    interval_s: float = 0.0,
    now: Callable[[], float] = time.time,
    lock_timeout_s: float = DEFAULT_LOCK_TIMEOUT_S,
) -> dict:
    """Publish the conformance fixture end to end: schema, batches, heartbeat, event.

    Uses the robot's REAL spool lanes (`spool.append_next`/`ack`), in the
    exact append -> publish -> ack order `StreamRouter._pump` uses --
    durable before sent, acked only after the broker has QoS-1 acknowledged
    it. Every channel batch and the event use `Spool.append_next()`, never a
    separate `next_seq()` + `append()` pair (Codex round 3 P1, comment
    3924082774 -- see `Spool.append_next`'s and `next_seq`'s docstrings for
    why that two-call shape is unsafe: it left a gap between allocation and
    write that a concurrent writer's `exclusive()`-held run could land in).
    On any exception once appending has begun, every lane this run wrote to
    is acked past the last seq it appended (advance-to semantics, per
    `Spool.ack`) before the exception is re-raised, so nothing this run
    spooled lingers for the real fleet service to replay later.

    Before any of that: refuses to start at all if either lane already has
    pending unacked backlog (`_check_no_pending_backlog`, C1 ruling) --
    otherwise this run's own first ack would advance the watermark past
    that pre-existing backlog and silently drop it.

    Immediately after connecting, but before any publish: refuses to
    proceed if the robot's live fleet session is already connected (Codex
    round 3 P1, comment 3927287968 -- see `_check_no_live_session`). Even
    non-retained and even under a distinct client id, this run's fixture
    schema would otherwise still reach a connected live subscriber as a
    schema update, remapping live channel batches to the fixture's
    `selftest.*` channels until the live service's next reconnect.

    That START-only check is not the end of it: right after it passes, an
    ONGOING watch is armed too (Codex round 3 follow-up, PR #214 P1,
    comment 3927287968's own follow-up -- see `_open_live_session_watch`)
    and re-checked between every channel batch and immediately before the
    heartbeat and event publishes (`_abort_if_live_session_detected`). A
    live `FleetService` that RESUMES at any point DURING the run -- not
    just one that was already connected at the start -- reopens the exact
    same schema-pollution window; the ongoing watch is what closes it for
    the run's full duration, not just its first instant.

    The entire run -- the backlog check and every `append_next`/`ack` call
    that follows -- holds `spool.exclusive()` (Codex round 3, P1b): a
    concurrently running `FleetService`/`StreamRouter` writing this same
    real spool waits for the selftest to finish instead of racing it for
    seqs. If a different writer already holds the lock for longer than
    `lock_timeout_s`, this refuses to start at all rather than hanging.
    Pausing the real service before running the selftest (see the runbook)
    remains good practice -- it avoids the brief wait -- but is no longer
    required for correctness, on either count (`exclusive()` for
    cross-instance serialization, `append_next()` for atomic allocation
    within whichever instance is currently writing).

    Args:
        publisher: A connected-or-connectable `Publisher` (typically
            `MqttPublisher`).
        spool: The target robot's real `Spool` (see `Spool.for_robot`).
        batches: Number of channel batches to publish.
        interval_s: Delay between batches (0 in tests; the CLI defaults this
            to 0.5s so a subscriber can observe batches arriving over time).
        now: Clock, injectable for deterministic tests.
        lock_timeout_s: Seconds to wait for `spool.exclusive()` before
            refusing to start (typed `SelftestPreconditionError`).

    Returns:
        `{"channels": 4, "batches", "samples", "heartbeat": 1, "events": 1,
        "channels_seq": [first, last], "events_seq": <seq>}`.

    Raises:
        SelftestPreconditionError: a spool lane has pending unacked backlog
            or another writer already holds the spool's lock (both checked
            before any connect/append side effect), the robot's live fleet
            session is already connected (checked right after connecting,
            before any publish), or a live session connects PARTWAY through
            the run (checked between batches and before the heartbeat/event
            publishes -- see `_abort_if_live_session_detected`).
        Whatever `publisher`/`spool` raise thereafter (typically
        `PublishError` or a spool `ValueError`/`SpoolError`) -- always
        re-raised after the cleanup below.

    """
    try:
        lock = spool.exclusive(timeout=lock_timeout_s)
    except SpoolLockedError as exc:
        raise SelftestPreconditionError(
            f"selftest refused: another writer holds the spool ({exc})"
        ) from exc

    with lock:
        _check_no_pending_backlog(spool)

        last_channels_seq: int | None = None
        last_events_seq: int | None = None
        channels_seqs: list[int] = []
        total_samples = 0
        t0 = now()

        try:
            publisher.connect()
            _check_no_live_session(publisher)
            watch = _open_live_session_watch(publisher)
            publisher.publish_schema({"v": 1, "channels": SELFTEST_CHANNELS})

            for i in range(batches):
                _abort_if_live_session_detected(watch, publisher)
                t = now()
                batch = _build_batch(i, t)
                seq = spool.append_next("channels", lambda seq, batch=batch: _stamp_seq(batch, seq))
                last_channels_seq = seq
                publisher.publish_channels(batch)
                spool.ack("channels", seq)
                channels_seqs.append(seq)
                total_samples += len(batch["samples"])
                if interval_s:
                    time.sleep(interval_s)

            _abort_if_live_session_detected(watch, publisher)
            heartbeat_payload = build_heartbeat(
                started_at=t0,
                subscriptions=["selftest"],
                channels_active=len(SELFTEST_CHANNELS),
                queue_depth=0,
                queue_dropped=0,
                spool_stats=spool.stats(),
                disk_free_bytes=disk_free(settings.CACHE_DIRECTORY),
                reconnects=0,
                now=now(),
            )
            publisher.publish_heartbeat(heartbeat_payload)

            event_payload: dict = {}

            def _build_event_payload(seq: int) -> dict:
                nonlocal event_payload
                event_payload = {
                    "v": 1,
                    "seq": seq,
                    "event_id": f"selftest-{uuid.uuid4()}",
                    "name": "selftest",
                    "t_start": t0,
                    "t_end": now(),
                    "source_topic": "selftest",
                    "summary": {"kind": "selftest", "batches": batches},
                }
                return event_payload

            _abort_if_live_session_detected(watch, publisher)
            events_seq = spool.append_next("events", _build_event_payload)
            last_events_seq = events_seq
            publisher.publish_event(event_payload)
            spool.ack("events", events_seq)

            if watch is not None:
                watch.stop()  # type: ignore[attr-defined]
            publisher.close()
        except Exception:
            if last_channels_seq is not None:
                spool.ack("channels", last_channels_seq)
            if last_events_seq is not None:
                spool.ack("events", last_events_seq)
            raise

        return {
            "channels": len(SELFTEST_CHANNELS),
            "batches": batches,
            "samples": total_samples,
            "heartbeat": 1,
            "events": 1,
            "channels_seq": [channels_seqs[0], channels_seqs[-1]] if channels_seqs else [],
            "events_seq": events_seq,
        }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.sink.publish.selftest",
        description=(
            "Fleet conformance selftest (spec §8): publish a known "
            "channel/heartbeat/event sequence to prove wire-protocol "
            "ingestion end to end, with no robot required."
        ),
    )
    parser.add_argument(
        "--broker",
        default=None,
        help="mqtt(s):// broker override, for dev rigs (defaults to the manifest/identity broker)",
    )
    parser.add_argument(
        "--batches", type=int, default=DEFAULT_BATCHES, help="channel batches to publish"
    )
    parser.add_argument(
        "--interval-s",
        type=float,
        default=DEFAULT_INTERVAL_S,
        help="delay between batches, seconds",
    )
    return parser


_EXPECTED_ERRORS = (
    FleetDisabledError,
    FleetNotInstalledError,
    FleetNotEnrolledError,
    StreamConfigError,
    PublishError,
    ValueError,
)


def _load_identity_or_none(directory: pathlib.Path) -> Identity | None:
    """Single-load `load_identity`, `None` on `FleetNotEnrolledError` (M4).

    Mirrors `control._load_identity_or_none` -- NOT an `is_enrolled()` check
    followed by a separate `load_identity()` call, which has a TOCTOU window:
    a corrupt/deleted identity between the two calls would raise
    `FleetNotEnrolledError` uncaught instead of degrading to "unenrolled".
    One `load_identity()` call, caught here, closes that window structurally.
    """
    try:
        return load_identity(directory)
    except FleetNotEnrolledError:
        return None


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: run the selftest against a resolved fleet publisher.

    Returns 0 and prints the `run_selftest` summary as one line of JSON on
    success. On any expected failure state (fleet disabled/not installed,
    unenrolled with no `--broker` override, bad broker config, a publish
    failure, a spool lane with pending backlog, another writer already
    holding the spool's lock, or the robot's live fleet session already
    being connected), prints the typed error's message to stderr (no
    traceback) and returns 1.
    """
    args = _build_arg_parser().parse_args(argv)
    try:
        require_fleet()
        directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
        identity = _load_identity_or_none(directory)
        kwargs = resolve_publisher_kwargs(StreamsConfig(broker=args.broker), identity)
        # "/selftest" (Codex round 3 follow-up, PR #214 P2 on comment
        # 3925391258; delimiter fixed to "/" in a later follow-up, comment
        # 3927231074): without this, the selftest's MqttPublisher would
        # derive the SAME deterministic client id as the live service's own
        # (same tenant/robot), and the broker kicks the existing session
        # when a new connection claims an already-connected client id --
        # running the selftest against an enrolled robot's broker while
        # that robot's real streaming service is connected would silently
        # DISPLACE the live session. Cloud confirmed ACLs key on the cert
        # CN, not the client id, so this is free. "/" (not "-") because a
        # hyphen reintroduces exactly the hyphen-injectivity ambiguity the
        # tenant/robot delimiter itself was already fixed for: robot "r7"
        # suffixed "-selftest" would collide with a robot actually NAMED
        # "r7-selftest". "/" is outside both id charsets (robot ids match
        # ^[a-z0-9][a-z0-9_-]{0,62}$), so it's provably collision-free the
        # same way. See `MqttPublisher.__init__`'s docstring for the full
        # reasoning.
        kwargs["client_id_suffix"] = "/selftest"
        # retain_messages=False (Codex round 3 follow-up, PR #214 P1 on
        # comment 3927023413): the selftest keeps publishing AS the robot
        # (client-id suffix aside, cert-CN ACLs make an isolated identity a
        # non-starter), so its retained publishes would otherwise linger on
        # the SAME shared robot topics with nothing to overwrite them once
        # the run ends -- its fixture schema staying retained until the
        # live service's next reconnect (a late subscriber decodes live
        # batches against the WRONG schema meanwhile), and its close() beat
        # leaving a retained online:false (the robot looks dead until the
        # next live beat). Retention isn't load-bearing for conformance --
        # the validator subscribes before/during the run. See
        # `MqttPublisher.__init__`'s docstring for the full reasoning.
        kwargs["retain_messages"] = False
        publisher = MqttPublisher(**kwargs)
        spool = Spool.for_robot(identity.robot if identity is not None else "dev/robot")
        result = run_selftest(publisher, spool, batches=args.batches, interval_s=args.interval_s)
    except _EXPECTED_ERRORS as exc:
        print(str(exc), file=sys.stderr)  # noqa: T201 -- the CLI's stderr contract
        return 1

    print(json.dumps(result))  # noqa: T201 -- the CLI's stdout contract (spec §8)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
