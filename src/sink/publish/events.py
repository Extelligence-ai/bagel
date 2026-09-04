"""Pure fleet event engine: predicates, rings, debounce, suppression, envelopes.

Fleet step 8, Task 5.

Mirrors `RouterCore`'s role for the router thread: no I/O, no threads, no
clock of its own -- callers pass `t`/`now` and drive `offer()`/`flush()`.
Task 7's emitter thread owns the clock, the artifact write, and publishing.

`validate_predicates` reuses `live.evaluate_predicate` with an empty `{}`
message to probe each rule's predicate at service start (an all-null one-row
relation), so a bad topic or a broken predicate (syntax error, unknown
column) surfaces as a `StreamConfigError` at `FleetService.start()` rather
than silently failing (or crashing the tap) on the first live message.

`EventEngine.offer` reuses `live.LiveEventTrigger` verbatim, per rule, for
the rising-edge + post-window + debounce logic -- that piece is already unit
tested in `test/pipeline/test_live.py`. What's new here: a per-topic ring
buffer of recent samples (rules sharing a topic share one ring, since a
firing's window is just a slice of that ring), a rolling-60s suppression gate
per rule, and the binding event envelope (`build_event_payload`).

Only `src.pipeline.live` (duckdb/pyarrow -- core, not optional) is imported
at module scope. Never `paho`/`cryptography` here -- see
`test_events_module_does_not_import_paho_or_cryptography_eagerly`.
"""

import json
import logging
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa

from src.pipeline import live
from src.sink.publish import StreamConfigError
from src.sink.publish.artifacts import ArtifactStore
from src.sink.publish.config import EventRule
from src.sink.publish.health import HealthInputs, HealthSnapshot, build_health_report
from src.sink.publish.provenance import build_provenance
from src.sink.publish.router import SampleQueue
from src.sink.publish.spool import Spool

logger = logging.getLogger(__name__)

HEALTH_SOURCE_TOPIC = "internal:health"

SUPPRESSION_WINDOW_SECONDS = 60.0
RING_SLACK_SECONDS = 5.0


def validate_predicates(rules: list[EventRule], structs: dict[str, pa.StructType]) -> None:
    """Probe every rule's predicate against its topic's schema at service start.

    Pure module function: it needs nothing service-internal, only the rules
    and a `topic -> struct` mapping (built from any sink's
    `buffer_writer(topic).struct`), so a config-swap path can pre-validate an
    incoming manifest before tearing the old service down.

    For each rule: a topic missing from `structs` raises `StreamConfigError`
    naming `events[i].topic`; a topic whose struct has no fields at all also
    raises naming `events[i].topic`, BEFORE the duckdb probe (a fieldless
    struct poisons the shared duckdb connection one-shot -- see Task-5 F1).
    Otherwise, `live.evaluate_predicate` is called
    with an empty `{}` message (an all-null one-row relation) inside a broad
    `try/except`; any exception (bad SQL syntax, an unknown column) raises
    `StreamConfigError` naming `events[i].predicate`, wrapping the original
    message. A predicate that merely evaluates to a null/false result on the
    all-null probe row is not an error -- only a raised exception is.

    A duplicate `rule.name` also raises, naming `events[i].name`: `EventEngine.
    __init__` indexes per-rule state (`_RuleState` -- the trigger, suppression
    deque, counters) by name, so two rules sharing a name would silently
    collapse into one shared state -- possibly even across different topics --
    with the LAST rule's window settings controlling both (Codex review).
    """
    seen_names: set[str] = set()
    for i, rule in enumerate(rules):
        if rule.name in seen_names:
            raise StreamConfigError(f"events[{i}].name", f"duplicate event name '{rule.name}'")
        seen_names.add(rule.name)
        if rule.topic not in structs:
            raise StreamConfigError(f"events[{i}].topic", f"not subscribed to topic '{rule.topic}'")
        struct = structs[rule.topic]
        # Task-5 F1: a fieldless struct poisons the shared duckdb connection
        # one-shot, so it must be refused BEFORE the probe below ever runs.
        if struct.num_fields == 0:
            raise StreamConfigError(f"events[{i}].topic", "topic has no fields to evaluate")
        try:
            live.evaluate_predicate(rule.topic, struct, {}, rule.predicate)
        except Exception as exc:
            raise StreamConfigError(f"events[{i}].predicate", str(exc)) from exc


@dataclass(frozen=True)
class Firing:
    """One released event: a rule's rising edge plus its captured window."""

    rule: EventRule
    t_event: float
    t_end: float
    window: list[tuple[float, dict]]
    suppressed: int


class _RuleState:
    """Per-rule mutable state: the trigger, the suppression clock, and counters."""

    __slots__ = ("fired_at", "predicate_errors", "suppressed_since_last", "trigger")

    def __init__(self, rule: EventRule) -> None:
        self.trigger = live.LiveEventTrigger(
            forward_seconds=rule.post_seconds, debounce_seconds=rule.debounce_seconds
        )
        self.fired_at: deque[float] = deque()  # accepted-firing timestamps, trailing 60s
        self.suppressed_since_last = 0
        self.predicate_errors = 0


class EventEngine:
    """Pure, thread-free evaluation of `events:` rules over a live sample stream.

    One ring buffer per source topic (shared by every rule on that topic);
    one `live.LiveEventTrigger` plus a rolling-minute suppression gate per
    rule. `offer()` is the hot path (called once per sample from the buffer
    tap); `flush()` mirrors it for stream end / `TopicBufferWriter.
    flush_pending_events`'s precedent -- events still waiting on their
    post-window fire best-effort with whatever the ring holds.
    """

    def __init__(
        self,
        rules: list[EventRule],
        structs: dict[str, pa.StructType],
        *,
        max_per_minute: int,
        ring_max_samples: int,
        ring_max_bytes: int,
    ) -> None:
        """Index rules by topic and set up one ring + retention window per topic."""
        self._rules = list(rules)
        self._structs = structs
        self._max_per_minute = max_per_minute
        self._ring_max_samples = ring_max_samples
        self._ring_max_bytes = ring_max_bytes

        self._states: dict[str, _RuleState] = {rule.name: _RuleState(rule) for rule in rules}
        self._rules_by_topic: dict[str, list[EventRule]] = {}
        for rule in rules:
            self._rules_by_topic.setdefault(rule.topic, []).append(rule)

        self._rings: dict[str, deque[tuple[float, dict, int]]] = {
            topic: deque() for topic in self._rules_by_topic
        }
        self._ring_bytes: dict[str, int] = dict.fromkeys(self._rules_by_topic, 0)
        self._retain_seconds: dict[str, float] = {
            topic: max(r.pre_seconds + r.post_seconds for r in topic_rules) + RING_SLACK_SECONDS
            for topic, topic_rules in self._rules_by_topic.items()
        }

        self._fired = 0
        self._suppressed = 0
        self._last_event_at: float | None = None
        self._last_seen_t: dict[str, float] = {}

    def offer(self, topic: str, t: float, msg: dict) -> list[Firing]:
        """Feed one sample: append to its topic's ring, evaluate each rule on it.

        Returns any Firings released as of this sample (edges whose post
        window has now elapsed and that passed the suppression gate).

        Source clocks are not guaranteed monotonic -- simulated time can
        reset or jump backward across a log/replay boundary -- but `live.
        LiveEventTrigger.feed` requires non-decreasing timestamps for its
        debounce/window arithmetic; fed a regressing `t` raw, a genuine new
        edge could be silently discarded (Codex review, P1). So the
        predicate is always evaluated against this sample's own `msg` (its
        values still matter, however late it arrived), but the trigger is
        fed `max(t, last_seen)` per topic, so time as the trigger sees it
        never runs backward. The ring keeps the sample's real `t` (window
        slicing wants the true arrival order); only the trigger feed and the
        resulting Firing's `t_event`/`t_end` are clamped.
        """
        if topic not in self._rules_by_topic:
            return []

        self._append_to_ring(topic, t, msg)

        last_seen = self._last_seen_t.get(topic)
        feed_t = t if last_seen is None else max(t, last_seen)
        self._last_seen_t[topic] = feed_t

        firings: list[Firing] = []
        for rule in self._rules_by_topic[topic]:
            state = self._states[rule.name]
            hit = self._safe_evaluate(rule, state, topic, msg)
            for t_event in state.trigger.feed(feed_t, hit):
                firing = self._release(rule, state, t_event, feed_t)
                if firing is not None:
                    firings.append(firing)
        return firings

    def flush(self, now: float) -> list[Firing]:
        """Fire, best-effort, every rule's events still waiting on their post window."""
        firings: list[Firing] = []
        for rule in self._rules:
            state = self._states[rule.name]
            for t_event in state.trigger.flush():
                firing = self._release(rule, state, t_event, now)
                if firing is not None:
                    firings.append(firing)
        return firings

    def counters(self) -> dict:
        """Return `{"fired", "suppressed", "predicate_errors", "last_event_at"}`.

        `predicate_errors` is a dict keyed by rule name.
        """
        return {
            "fired": self._fired,
            "suppressed": self._suppressed,
            "predicate_errors": {
                rule.name: self._states[rule.name].predicate_errors for rule in self._rules
            },
            "last_event_at": self._last_event_at,
        }

    def _append_to_ring(self, topic: str, t: float, msg: dict) -> None:
        approx_bytes = len(json.dumps(msg))
        ring = self._rings[topic]
        ring.append((t, msg, approx_bytes))
        self._ring_bytes[topic] += approx_bytes
        self._prune_ring(topic, t)

    def _prune_ring(self, topic: str, latest_t: float) -> None:
        ring = self._rings[topic]
        retain = self._retain_seconds[topic]
        cutoff = latest_t - retain

        def _pop_oldest() -> None:
            _, _, sample_bytes = ring.popleft()
            self._ring_bytes[topic] -= sample_bytes

        while ring and ring[0][0] < cutoff:
            _pop_oldest()
        while len(ring) > self._ring_max_samples:
            _pop_oldest()
        while ring and self._ring_bytes[topic] > self._ring_max_bytes:
            _pop_oldest()

    def _safe_evaluate(self, rule: EventRule, state: _RuleState, topic: str, msg: dict) -> bool:
        struct = self._structs[topic]
        try:
            return live.evaluate_predicate(topic, struct, msg, rule.predicate)
        except Exception:
            state.predicate_errors += 1
            log = logger.warning if state.predicate_errors == 1 else logger.debug
            log(
                "events[%s]: predicate raised on a live message; treating as no-hit",
                rule.name,
                exc_info=True,
            )
            return False

    def _release(
        self, rule: EventRule, state: _RuleState, t_event: float, t_end: float
    ) -> Firing | None:
        if not self._gate(state, t_event):
            return None
        self._fired += 1
        self._last_event_at = t_event
        # Reads, but deliberately does NOT clear, suppressed_since_last: it
        # is only cleared via `ack_suppressed`, once the caller has durably
        # spooled this Firing (Codex review) -- clearing it here, before
        # the append even happens, would lose the count if that append
        # then failed.
        suppressed = state.suppressed_since_last
        window = self._window_slice(rule, t_event, t_end)
        return Firing(rule=rule, t_event=t_event, t_end=t_end, window=window, suppressed=suppressed)

    def ack_suppressed(self, rule_name: str, amount: int) -> None:
        """Clear `amount` off a rule's pending suppressed-since-last count.

        Called by `EventEmitter` only once a `Firing`'s envelope has been
        durably appended to the spool -- see `_release`'s docstring. A
        no-op for an unknown rule name (defensive; should not happen in
        practice since names come from the engine's own rules).
        """
        state = self._states.get(rule_name)
        if state is not None:
            state.suppressed_since_last = max(0, state.suppressed_since_last - amount)

    def _gate(self, state: _RuleState, t_event: float) -> bool:
        while state.fired_at and t_event - state.fired_at[0] >= SUPPRESSION_WINDOW_SECONDS:
            state.fired_at.popleft()
        if len(state.fired_at) >= self._max_per_minute:
            state.suppressed_since_last += 1
            self._suppressed += 1
            return False
        state.fired_at.append(t_event)
        return True

    def _window_slice(
        self, rule: EventRule, t_event: float, t_end: float
    ) -> list[tuple[float, dict]]:
        start = t_event - rule.pre_seconds
        return [(t, msg) for t, msg, _ in self._rings[rule.topic] if start <= t <= t_end]


def build_event_payload(
    firing: Firing,
    *,
    seq: int,
    event_id: str,
    artifact_uri: str | None = None,
    artifact_error: str | None = None,
) -> dict:
    """Build the binding event envelope for one Firing (pure, module function).

    `event_id` is a parameter, not generated here: the emitter (Task 7) must
    allocate it BEFORE payload assembly so the artifact filename and the
    envelope share the same id.
    """
    rule = firing.rule
    summary: dict = {
        "predicate": rule.predicate,
        "pre_seconds": rule.pre_seconds,
        "post_seconds": rule.post_seconds,
        "debounce_seconds": rule.debounce_seconds,
        "samples": len(firing.window),
    }
    if firing.suppressed > 0:
        summary["suppressed"] = firing.suppressed
    if artifact_error is not None:
        summary["artifact_error"] = artifact_error
    build = build_provenance()
    if build is not None:
        summary["build"] = build

    payload: dict = {
        "v": 1,
        "seq": seq,
        "event_id": event_id,
        "name": rule.name,
        "t_start": firing.t_event,
        "t_end": firing.t_end,
        "source_topic": rule.topic,
        "summary": summary,
    }
    if artifact_uri is not None:
        payload["artifact"] = {"kind": "mcap", "uri": artifact_uri}
    return payload


class EventEmitter(threading.Thread):
    """Drains the event tap queue into the engine and spools whatever fires.

    Task 7's thread half, following the repo's thread-lifecycle precedent
    (`service.py` module docstring): daemon=True, a `threading.Event` stop
    signal, a bounded `join` in `stop()`, and a `StreamRouter`-style
    fatal-catch so an unhandled `_tick` bug is visible via `alive`/
    `last_error` instead of dying silently.

    ALWAYS built and started by `FleetService`, even with zero event rules:
    this thread owns the scheduled `health_report` (settle delay, then a
    fixed interval), which must run whether or not any `events:` rules are
    configured. Everything it appends goes to the never-drop `events` spool
    lane, append-only -- the router pumps and acks that lane (Task 3); a
    spool append failure logs WARNING and increments `spool_failures`
    (mirroring `HeartbeatThread._tick`'s posture), never killing the thread.
    """

    def __init__(  # noqa: PLR0913 -- one knob per collaborator, mirrors the plan's signature
        self,
        engine: EventEngine,
        queue: SampleQueue,
        spool: Spool,
        *,
        artifact_store: ArtifactStore | None,
        health_inputs: Callable[[], HealthInputs],
        structs: dict[str, pa.StructType],
        health_interval_s: float,
        health_settle_s: float,
        now: Callable[[], float] = time.time,
    ) -> None:
        """Wire the engine, queue, spool and health closure; does not start the thread.

        `now` is injectable (tests drive the health schedule with a fake
        clock); everything time-based in this thread reads it, never
        `time.time()` directly.
        """
        super().__init__(daemon=True)
        self._engine = engine
        self._queue = queue
        self._spool = spool
        self._artifact_store = artifact_store
        self._health_inputs = health_inputs
        self._structs = structs
        self._health_interval_s = health_interval_s
        self._health_settle_s = health_settle_s
        self._now = now
        self._stop_event = threading.Event()
        self._fatal_error: str | None = None
        self._last_error: str | None = None
        self._spool_failures = 0
        self._health_prev: HealthSnapshot | None = None
        self._started_at: float | None = None
        self._last_report_at: float | None = None
        self._next_health_at: float | None = None

    def run(self) -> None:
        """Arm the health schedule, then tick until stop() -- fatal-catch like the router."""
        self._started_at = self._now()
        self._next_health_at = self._started_at + self._health_settle_s
        while not self._stop_event.is_set():
            try:
                self._tick(self._now())
            except Exception as exc:
                logger.exception("EventEmitter thread died")
                self._fatal_error = repr(exc)
                return

    def _tick(self, now: float) -> None:
        """One iteration: drain the queue into the engine, emit firings, maybe report health.

        `queue.drain`'s bounded timeout paces the loop in `run()` exactly as
        it does for `StreamRouter._tick`. The health schedule advances by
        `health_interval_s` after every attempt -- success or a skipped
        report alike (`_emit_health`'s gatherer failure is retried at the
        NEXT interval, never rescheduled early).
        """
        for topic, t, msg in self._queue.drain(max_items=500, timeout_s=0.2):
            for firing in self._engine.offer(topic, t, msg):
                self._emit(firing)
        if self._next_health_at is not None and now >= self._next_health_at:
            self._emit_health(now)
            self._next_health_at += self._health_interval_s

    def _emit(self, firing: Firing) -> None:
        """Store the artifact (if any), then append the binding envelope to the events lane.

        `event_id` is allocated HERE, before the store call, so the artifact
        filename and the envelope share it (`build_event_payload` takes it as
        a parameter for exactly this reason). The window is sorted by `t`
        before storing (Task-5 F3): out-of-order sources produce unsorted
        ring windows, and MCAP readers assume chronological log time.

        The event ALWAYS goes out (Global Constraints ruling): an artifact
        failure -- the store's byte budget (`None` return) or a raised
        exception -- becomes `summary.artifact_error`, never a dropped event.
        """
        event_id = str(uuid.uuid4())
        uri: str | None = None
        artifact_error: str | None = None
        rule = firing.rule
        if rule.artifact == "mcap" and self._artifact_store is not None:
            window = sorted(firing.window, key=lambda sample: sample[0])
            try:
                path = self._artifact_store.store(
                    rule.name, event_id, rule.topic, self._structs[rule.topic], window
                )
            except Exception as exc:
                artifact_error = str(exc)
                logger.warning("events[%s]: artifact store failed", rule.name, exc_info=True)
            else:
                if path is None:
                    artifact_error = "artifact byte budget exceeded"
                else:
                    uri = path.as_uri()
        success = self._append_to_events_lane(
            lambda seq: build_event_payload(
                firing, seq=seq, event_id=event_id, artifact_uri=uri, artifact_error=artifact_error
            )
        )
        if success:
            # Only clear the rule's suppressed-since-last count once this
            # Firing's envelope has actually been delivered (Codex review):
            # clearing it earlier (inside the engine's own `_release`)
            # would lose the count on an append failure.
            self._engine.ack_suppressed(rule.name, firing.suppressed)

    def _emit_health(self, now: float) -> None:
        """Build one scheduled health report and append it to the events lane.

        A failing inputs gatherer skips this report (WARNING, `last_error`
        set) -- never kills the thread; `_tick` still advances the schedule,
        so the retry lands at the next interval. `t_start`/`t_end` span the
        period the report's delta counters cover (the previous report time --
        or the runtime start for the first report -- up to `now`);
        `source_topic` `"internal:health"` marks robot-internal events
        (contract doc paragraph 7).

        `_health_prev` and `_last_report_at` are only committed to their new
        values once the report is actually appended (Codex review): if the
        append fails (spool full, transient disk error --
        `_append_to_events_lane` swallows it), committing anyway would make
        the NEXT delivered report compute its deltas from this undelivered
        report's snapshot and skip `t_start` ahead to this report's `now`,
        silently dropping the missing interval's queue drops, evictions,
        reconnects, and heartbeat failures from every future report. Keeping
        the old baseline means the next successful report's period simply
        spans both the failed and the successful attempt.
        """
        try:
            inputs = self._health_inputs()
        except Exception as exc:
            self._last_error = repr(exc)
            logger.warning("health inputs gatherer failed; skipping this report", exc_info=True)
            return
        self._last_error = None
        summary, new_health_prev = build_health_report(inputs, self._health_prev, now=now)
        build = build_provenance()
        if build is not None:
            summary["build"] = build
        t_start = self._last_report_at if self._last_report_at is not None else self._started_at
        success = self._append_to_events_lane(
            lambda seq: {
                "v": 1,
                "seq": seq,
                "event_id": str(uuid.uuid4()),
                "name": "health_report",
                "t_start": t_start,
                "t_end": now,
                "source_topic": HEALTH_SOURCE_TOPIC,
                "summary": summary,
            }
        )
        if success:
            self._health_prev = new_health_prev
            self._last_report_at = now

    def _append_to_events_lane(self, build: Callable[[int], dict]) -> bool:
        """Atomically allocate a seq, build the payload with it, and append -- never-drop lane.

        Goes through `Spool.append_next()` (post-#214 fusion), never a
        separate `next_seq()` + `append()` pair: the payload embeds its own
        seq, and the two-call shape leaves a gap where a competing writer
        (the router's batch flush, a selftest's `exclusive()`-held run) can
        allocate a colliding seq or stale the floor between the calls --
        see `Spool.append_next`'s docstring. `build(seq)` runs inside the
        spool lock, so it must stay small and non-blocking (both callers
        only stamp an already-computed dict).

        Append-only -- the router pumps and acks (Task 3). A failure
        (`SpoolFullError` -- disk full on a never-drop lane -- or anything
        else from the spool path) logs WARNING and increments
        `spool_failures` (surfaced in status and the `events_pipeline` health
        metrics), mirroring `HeartbeatThread._tick`'s posture.

        Returns whether the append succeeded: callers (`_emit`, `_emit_
        health`) use this to gate state that must only advance on a durable
        append -- the suppressed-count drain and the health baseline commit
        (Codex review) -- rather than committing eagerly and losing that
        state on a failure this method already swallowed.
        """
        try:
            self._spool.append_next("events", build)
        except Exception:
            self._spool_failures += 1
            logger.warning("events spool append failed on lane 'events'", exc_info=True)
            return False
        return True

    def stop(self) -> None:
        """Signal the loop to stop, join bounded, then final-flush on the caller's thread.

        The final flush is guarded the same way `StreamRouter._final_flush`
        is: it only runs once the thread has provably exited its loop (a
        clean join, no fatal error), so it can never race a concurrent
        `_tick` touching the same engine/queue/spool. It drains the queue,
        offers everything, then `engine.flush(now)`s so events still waiting
        on their post window fire best-effort (never-drop philosophy). No
        final health report -- a stop is not a schedule.

        The join timeout (12s) matches `StreamRouter.stop`'s own bound
        (rider d): both threads can be blocked inside the same
        `MqttPublisher.publish`'s 10s QoS-1 `wait_for_publish` wait, so a
        shorter join here could return before termination was actually
        guaranteed, same as the router's own Codex-review fix.
        """
        self._stop_event.set()
        self.join(timeout=12)
        if self.is_alive():
            logger.warning("event emitter thread did not terminate")
            return
        if self._fatal_error is not None:
            return  # died mid-tick; engine/queue state is suspect, no final flush
        self._final_flush()

    def _final_flush(self) -> None:
        """Drain-then-offer-then-flush, once, after the thread loop has exited.

        Wrapped in a broad try/except (Codex review, rider c): unlike a
        mid-tick failure (caught by `run()`'s own fatal-catch and reported
        via `alive`/`last_error`), a failure HERE would otherwise propagate
        straight out of `stop()` -- called synchronously from
        `FleetService.stop()`/`pause()`'s own teardown -- and could abort
        that teardown before the publisher is released. Any exception is
        logged at ERROR and recorded via `last_error` instead, so `stop()`
        always completes.
        """
        try:
            while True:
                samples = self._queue.drain(max_items=500, timeout_s=0.0)
                if not samples:
                    break
                for topic, t, msg in samples:
                    for firing in self._engine.offer(topic, t, msg):
                        self._emit(firing)
            for firing in self._engine.flush(self._now()):
                self._emit(firing)
        except Exception as exc:
            logger.error("EventEmitter final flush failed", exc_info=True)
            self._last_error = repr(exc)

    def status_counters(self) -> dict:
        """Engine counters plus this thread's own queue/spool/health/liveness state."""
        return {
            **self._engine.counters(),
            "queue_depth": self._queue.depth,
            "queue_dropped": self._queue.dropped,
            "spool_failures": self._spool_failures,
            "health": {
                "last_report_at": self._last_report_at,
                "next_report_at": self._next_health_at,
            },
            "alive": self.alive,
            "last_error": self.last_error,
        }

    @property
    def alive(self) -> bool:
        """Whether the thread is running AND has not died on an unhandled `_tick` error."""
        return self.is_alive() and self._fatal_error is None

    @property
    def last_error(self) -> str | None:
        """The fatal `_tick` error, or the most recent health-inputs failure, if any."""
        return self._fatal_error or self._last_error
