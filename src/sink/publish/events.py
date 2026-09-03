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
from collections import deque
from dataclasses import dataclass

import pyarrow as pa

from src.pipeline import live
from src.sink.publish import StreamConfigError
from src.sink.publish.config import EventRule
from src.sink.publish.provenance import build_provenance

logger = logging.getLogger(__name__)

SUPPRESSION_WINDOW_SECONDS = 60.0
RING_SLACK_SECONDS = 5.0


def validate_predicates(rules: list[EventRule], structs: dict[str, pa.StructType]) -> None:
    """Probe every rule's predicate against its topic's schema at service start.

    For each rule: a topic missing from `structs` raises `StreamConfigError`
    naming `events[i].topic`. Otherwise, `live.evaluate_predicate` is called
    with an empty `{}` message (an all-null one-row relation) inside a broad
    `try/except`; any exception (bad SQL syntax, an unknown column) raises
    `StreamConfigError` naming `events[i].predicate`, wrapping the original
    message. A predicate that merely evaluates to a null/false result on the
    all-null probe row is not an error -- only a raised exception is.
    """
    for i, rule in enumerate(rules):
        if rule.topic not in structs:
            raise StreamConfigError(f"events[{i}].topic", f"not subscribed to topic '{rule.topic}'")
        struct = structs[rule.topic]
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

    def offer(self, topic: str, t: float, msg: dict) -> list[Firing]:
        """Feed one sample: append to its topic's ring, evaluate each rule on it.

        Returns any Firings released as of this sample (edges whose post
        window has now elapsed and that passed the suppression gate).
        """
        if topic not in self._rules_by_topic:
            return []

        self._append_to_ring(topic, t, msg)

        firings: list[Firing] = []
        for rule in self._rules_by_topic[topic]:
            state = self._states[rule.name]
            hit = self._safe_evaluate(rule, state, topic, msg)
            for t_event in state.trigger.feed(t, hit):
                firing = self._release(rule, state, t_event, t)
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
        suppressed = state.suppressed_since_last
        state.suppressed_since_last = 0
        window = self._window_slice(rule, t_event, t_end)
        return Firing(rule=rule, t_event=t_event, t_end=t_end, window=window, suppressed=suppressed)

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
