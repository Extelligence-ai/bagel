"""Tests for the fleet event engine and its emitter thread (fleet step 8, Tasks 5+7).

The engine half is pure -- synthetic structs via `pa.struct`, no threads,
clock-injected via the `t`/`now` arguments the engine's methods take. The
emitter half (Task 7) runs the real `EventEmitter` thread against a real tmp
`Spool` and (where relevant) a real `ArtifactStore` -- no broker anywhere.
"""

import importlib
import pathlib
import sys
import time
import uuid

import pyarrow as pa
import pytest

from publish.conftest import _wait_until
from settings import settings
from src.pipeline import live
from src.sink.publish import StreamConfigError, events
from src.sink.publish.artifacts import ArtifactStore
from src.sink.publish.config import EventRule
from src.sink.publish.health import HealthInputs
from src.sink.publish.router import SampleQueue
from src.sink.publish.spool import Spool, SpoolFullError

IMU_STRUCT = pa.struct([("accel_x", pa.float64())])
BLOB_STRUCT = pa.struct([("accel_x", pa.float64()), ("note", pa.string())])

# The binding envelope's top-level key set (spec §7) -- pinned exactly, so an
# accidentally added or dropped top-level key fails loudly.
ENVELOPE_BASE_KEYS = {"v", "seq", "event_id", "name", "t_start", "t_end", "source_topic", "summary"}


def _rule(  # noqa: PLR0913 -- one field per EventRule window/predicate knob, kept explicit
    name: str = "hard_decel",
    topic: str = "imu",
    predicate: str = "imu['accel_x'] < -10",
    pre_seconds: float = 0.0,
    post_seconds: float = 0.0,
    debounce_seconds: float = 0.0,
    artifact: str | None = None,
) -> EventRule:
    return EventRule(
        name=name,
        topic=topic,
        predicate=predicate,
        pre_seconds=pre_seconds,
        post_seconds=post_seconds,
        debounce_seconds=debounce_seconds,
        artifact=artifact,
    )


def test_events_module_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """events.py must not drag paho or cryptography at import time.

    src.pipeline.live (duckdb/pyarrow) IS fine to import eagerly -- it's core,
    not an optional fleet dependency.
    """
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.events", raising=False)
    importlib.import_module("src.sink.publish.events")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )


class TestBuildEventPayload:
    """The binding envelope shape (spec's summary.build)."""

    def _firing(self, **overrides: object) -> events.Firing:
        rule = overrides.pop("rule", None) or _rule(
            pre_seconds=2.0, post_seconds=2.0, debounce_seconds=1.0
        )
        defaults = dict(
            rule=rule,
            t_event=10.0,
            t_end=12.0,
            window=[(9.0, {"accel_x": -1.0}), (12.0, {"accel_x": -12.0})],
            suppressed=0,
        )
        defaults.update(overrides)
        return events.Firing(**defaults)

    def test_top_level_keys_exactly(self) -> None:
        firing = self._firing()
        payload = events.build_event_payload(firing, seq=1, event_id=str(uuid.uuid4()))
        assert set(payload) == ENVELOPE_BASE_KEYS

    def test_scalar_fields(self) -> None:
        firing = self._firing()
        event_id = str(uuid.uuid4())
        payload = events.build_event_payload(firing, seq=7, event_id=event_id)
        assert payload["v"] == 1
        assert payload["seq"] == 7
        assert payload["event_id"] == event_id
        uuid.UUID(payload["event_id"])  # parses as a UUID
        assert payload["name"] == "hard_decel"
        assert payload["t_start"] == 10.0
        assert payload["t_end"] == 12.0
        assert payload["source_topic"] == "imu"

    def test_summary_fields_and_no_suppressed_when_zero(self) -> None:
        firing = self._firing()
        payload = events.build_event_payload(firing, seq=1, event_id=str(uuid.uuid4()))
        summary = payload["summary"]
        assert summary["predicate"] == firing.rule.predicate
        assert summary["pre_seconds"] == 2.0
        assert summary["post_seconds"] == 2.0
        assert summary["debounce_seconds"] == 1.0
        assert summary["samples"] == 2
        assert "suppressed" not in summary
        assert "artifact_error" not in summary
        assert "build" not in summary
        assert "artifact" not in payload

    def test_suppressed_included_when_nonzero(self) -> None:
        firing = self._firing(suppressed=3)
        payload = events.build_event_payload(firing, seq=1, event_id=str(uuid.uuid4()))
        assert payload["summary"]["suppressed"] == 3

    def test_artifact_error_included_when_set(self) -> None:
        firing = self._firing()
        payload = events.build_event_payload(
            firing, seq=1, event_id=str(uuid.uuid4()), artifact_error="disk full"
        )
        assert payload["summary"]["artifact_error"] == "disk full"
        assert "artifact" not in payload

    def test_artifact_uri_produces_artifact_block(self) -> None:
        firing = self._firing()
        payload = events.build_event_payload(
            firing, seq=1, event_id=str(uuid.uuid4()), artifact_uri="file:///tmp/x.mcap"
        )
        assert payload["artifact"] == {"kind": "mcap", "uri": "file:///tmp/x.mcap"}
        # Exact top-level key-set pin for the artifact branch too (Task-5 F2):
        # the artifact key is the ONLY addition over the base envelope.
        assert set(payload) == ENVELOPE_BASE_KEYS | {"artifact"}

    def test_build_absent_when_provenance_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", None)
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        firing = self._firing()
        payload = events.build_event_payload(firing, seq=1, event_id=str(uuid.uuid4()))
        assert "build" not in payload["summary"]

    def test_build_present_when_provenance_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "abc123")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", "deadbeef")
        firing = self._firing()
        payload = events.build_event_payload(firing, seq=1, event_id=str(uuid.uuid4()))
        assert payload["summary"]["build"] == {"build_id": "abc123", "vcs_ref": "deadbeef"}


class TestValidatePredicates:
    def test_unknown_topic_raises_naming_topic_field(self) -> None:
        rules = [_rule(topic="nope")]
        with pytest.raises(StreamConfigError) as exc_info:
            events.validate_predicates(rules, {"imu": IMU_STRUCT})
        assert exc_info.value.field == "events[0].topic"
        assert "not subscribed" in str(exc_info.value).lower()

    def test_bad_sql_raises_naming_predicate_field(self) -> None:
        rules = [_rule(predicate="this is not :: valid sql (((")]
        with pytest.raises(StreamConfigError) as exc_info:
            events.validate_predicates(rules, {"imu": IMU_STRUCT})
        assert exc_info.value.field == "events[0].predicate"

    def test_unknown_column_raises_naming_predicate_field(self) -> None:
        rules = [_rule(predicate="imu['nope_field'] < -10")]
        with pytest.raises(StreamConfigError) as exc_info:
            events.validate_predicates(rules, {"imu": IMU_STRUCT})
        assert exc_info.value.field == "events[0].predicate"

    def test_valid_predicate_passes(self) -> None:
        rules = [_rule(predicate="imu['accel_x'] < -10")]
        assert events.validate_predicates(rules, {"imu": IMU_STRUCT}) is None

    def test_index_in_field_name_matches_rule_position(self) -> None:
        rules = [_rule(name="ok", predicate="imu['accel_x'] < -10"), _rule(topic="nope")]
        with pytest.raises(StreamConfigError) as exc_info:
            events.validate_predicates(rules, {"imu": IMU_STRUCT})
        assert exc_info.value.field == "events[1].topic"

    def test_fieldless_struct_raises_naming_topic_before_the_probe(self) -> None:
        """Task-5 F1: a fieldless struct poisons the shared duckdb connection
        one-shot, so the guard must fire BEFORE the probe ever reaches duckdb."""
        rules = [_rule(predicate="true")]
        with pytest.raises(StreamConfigError) as exc_info:
            events.validate_predicates(rules, {"imu": pa.struct([])})
        assert exc_info.value.field == "events[0].topic"
        assert "no fields" in str(exc_info.value)
        # The probe never ran, so the shared duckdb connection is still fine:
        # a normal validation right after must still succeed.
        assert events.validate_predicates([_rule()], {"imu": IMU_STRUCT}) is None

    def test_duplicate_names_rejected_naming_the_duplicate_index(self) -> None:
        """Codex review (P2, events.py:140): duplicate names would otherwise
        collapse into one shared `_RuleState` in `EventEngine.__init__`
        (shared debounce/suppression/window state across rules), so
        validation must reject them before the engine is ever built."""
        rules = [_rule(name="dup"), _rule(name="dup", topic="imu")]
        with pytest.raises(StreamConfigError) as exc_info:
            events.validate_predicates(rules, {"imu": IMU_STRUCT})
        assert exc_info.value.field == "events[1].name"
        assert "duplicate" in str(exc_info.value).lower()

    def test_unique_names_still_pass(self) -> None:
        rules = [_rule(name="a"), _rule(name="b")]
        assert events.validate_predicates(rules, {"imu": IMU_STRUCT}) is None


class TestEdgeAndWindows:
    def test_single_firing_after_post_window_with_correct_window_slice(self) -> None:
        rule = _rule(pre_seconds=2.0, post_seconds=2.0, debounce_seconds=1.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        stream = [
            (0.0, -1.0),  # older content, must be pruned from the eventual window
            (3.0, -1.0),
            (5.0, -12.0),  # rising edge
            (6.0, -12.0),  # still hit -- not a new edge
            (7.0, -1.0),  # releases the post window (5 + 2 == 7)
        ]
        all_firings: list[events.Firing] = []
        for t, accel in stream:
            all_firings.extend(engine.offer("imu", t, {"accel_x": accel}))

        assert len(all_firings) == 1
        firing = all_firings[0]
        assert firing.rule is rule
        assert firing.t_event == 5.0
        assert firing.t_end == 7.0
        assert [t for t, _ in firing.window] == [3.0, 5.0, 6.0, 7.0]


class TestRegressingTimestamps:
    """Codex review (P1, events.py:173): a source clock is not guaranteed
    monotonic (e.g. simulated time can reset across a log boundary).
    `LiveEventTrigger.feed` requires non-decreasing timestamps for its
    debounce/window arithmetic -- fed a regressing `t` raw, a genuine new
    edge could be silently discarded (debounce math going negative) rather
    than raising. `EventEngine.offer` must clamp what it feeds the trigger
    to a per-topic monotonic maximum, without ever raising and without
    dropping a real edge."""

    def test_regressing_sample_does_not_raise_and_edge_still_fires(self) -> None:
        rule = _rule(post_seconds=0.0, debounce_seconds=0.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        firings: list[events.Firing] = []
        firings.extend(engine.offer("imu", 10.0, {"accel_x": -12.0}))  # edge 1, fires @10
        firings.extend(engine.offer("imu", 11.0, {"accel_x": -1.0}))  # false, resets edge
        # Regresses behind the topic's last-seen t=11.0 -- must not raise,
        # and (being a genuine rising edge) must still be detected.
        firings.extend(engine.offer("imu", 9.0, {"accel_x": -12.0}))

        assert [f.t_event for f in firings] == [10.0, 11.0]
        # Monotonic: the trigger never saw time run backward.
        assert firings[1].t_event >= firings[0].t_event
        assert firings[1].t_end >= firings[1].t_event

    def test_regressing_non_edge_sample_produces_no_false_firing(self) -> None:
        rule = _rule(post_seconds=0.0, debounce_seconds=0.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        engine.offer("imu", 10.0, {"accel_x": -12.0})  # edge 1, fires @10
        # A regressing sample that is NOT a hit must fire nothing.
        firings = engine.offer("imu", 9.0, {"accel_x": -1.0})
        assert firings == []


class TestDebounce:
    def test_close_edges_coalesce_into_one_firing(self) -> None:
        rule = _rule(debounce_seconds=1.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        stream = [
            (0.0, -1.0),
            (0.2, -12.0),  # edge 1
            (0.3, -1.0),
            (0.7, -12.0),  # 0.5s after edge 1 -- coalesced
            (0.8, -1.0),
        ]
        firings: list[events.Firing] = []
        for t, accel in stream:
            firings.extend(engine.offer("imu", t, {"accel_x": accel}))
        assert [f.t_event for f in firings] == [0.2]

    def test_far_edges_produce_two_firings(self) -> None:
        rule = _rule(debounce_seconds=1.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        stream = [
            (0.0, -1.0),
            (0.2, -12.0),  # edge 1
            (0.3, -1.0),
            (2.3, -12.0),  # 2.1s after edge 1 -- separate event
            (2.4, -1.0),
        ]
        firings: list[events.Firing] = []
        for t, accel in stream:
            firings.extend(engine.offer("imu", t, {"accel_x": accel}))
        assert [f.t_event for f in firings] == [0.2, 2.3]


class TestSuppression:
    def test_max_per_minute_suppresses_excess_edges_and_reports_on_next_firing(self) -> None:
        rule = _rule()
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=2,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        edges = [1.0, 3.0, 5.0, 7.0, 9.0]
        firings: list[events.Firing] = []
        for t in edges:
            firings.extend(engine.offer("imu", t, {"accel_x": -12.0}))
            firings.extend(engine.offer("imu", t + 0.1, {"accel_x": -1.0}))

        assert [f.t_event for f in firings] == [1.0, 3.0]
        assert [f.suppressed for f in firings] == [0, 0]
        counters = engine.counters()
        assert counters["fired"] == 2
        assert counters["suppressed"] == 3

        # Wait past the trailing 60s window so both earlier firings age out.
        firings.extend(engine.offer("imu", 65.0, {"accel_x": -12.0}))
        assert len(firings) == 3
        assert firings[-1].t_event == 65.0
        assert firings[-1].suppressed == 3

        counters = engine.counters()
        assert counters["fired"] == 3
        assert counters["suppressed"] == 3


class TestSuppressedCountPreservedUntilAck:
    """Codex review (P2, events.py:249): `_release` used to zero
    `suppressed_since_last` unconditionally, before the caller (the
    emitter) ever attempted to durably spool the resulting `Firing`. If
    that append then failed, the count was already gone -- the NEXT
    successful event silently omitted losses the protocol promises to
    report since the previous delivered firing. `_release` now only
    *reads* the counter; a caller must explicitly `ack_suppressed()` once
    the append has actually succeeded."""

    def test_release_reports_but_does_not_clear_the_counter(self) -> None:
        rule = _rule(post_seconds=0.0, debounce_seconds=0.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=1,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        engine.offer("imu", 1.0, {"accel_x": -12.0})  # fires, suppressed=0
        engine.offer("imu", 2.0, {"accel_x": -1.0})
        engine.offer("imu", 3.0, {"accel_x": -12.0})  # gated -> suppressed_since_last=1
        engine.offer("imu", 4.0, {"accel_x": -1.0})
        firings = engine.offer("imu", 65.0, {"accel_x": -12.0})  # ages out t=1.0, fires again
        assert firings[0].suppressed == 1

        # Simulate the caller never acknowledging this release (its append
        # "failed"): a SECOND release, with no further suppression in
        # between, must still report the SAME count -- not 0.
        engine.offer("imu", 66.0, {"accel_x": -1.0})
        firings2 = engine.offer("imu", 127.0, {"accel_x": -12.0})  # ages out t=65.0
        assert firings2[0].suppressed == 1

    def test_ack_suppressed_clears_only_the_acknowledged_amount(self) -> None:
        rule = _rule(post_seconds=0.0, debounce_seconds=0.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=1,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        engine.offer("imu", 1.0, {"accel_x": -12.0})
        engine.offer("imu", 2.0, {"accel_x": -1.0})
        engine.offer("imu", 3.0, {"accel_x": -12.0})  # gated -> suppressed_since_last=1
        engine.offer("imu", 4.0, {"accel_x": -1.0})
        firings = engine.offer("imu", 65.0, {"accel_x": -12.0})
        assert firings[0].suppressed == 1

        engine.ack_suppressed(rule.name, 1)

        engine.offer("imu", 66.0, {"accel_x": -1.0})
        firings2 = engine.offer("imu", 127.0, {"accel_x": -12.0})
        assert firings2[0].suppressed == 0  # cleared by the ack

    def test_ack_suppressed_unknown_rule_name_is_a_no_op(self) -> None:
        rule = _rule()
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        engine.ack_suppressed("nope", 1)  # must not raise


class TestRingBounds:
    def test_ring_max_samples_drops_oldest(self) -> None:
        rule = _rule(pre_seconds=100.0, post_seconds=0.0, debounce_seconds=0.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=3,
            ring_max_bytes=10_000_000,
        )
        firings: list[events.Firing] = []
        # Five no-hit samples first, ring cap 3 -> only the last 3 survive.
        for t in (1.0, 2.0, 3.0, 4.0, 5.0):
            engine.offer("imu", t, {"accel_x": -1.0})
        firings.extend(engine.offer("imu", 6.0, {"accel_x": -12.0}))  # rising edge, fires now

        assert len(firings) == 1
        window_times = [t for t, _ in firings[0].window]
        # Evicted samples at t=1..3 must not reappear; the ring never errors.
        assert 1.0 not in window_times
        assert 2.0 not in window_times

    def test_ring_max_bytes_drops_oldest(self) -> None:
        rule = _rule(
            topic="blob",
            predicate="blob['accel_x'] < -10",
            pre_seconds=100.0,
            post_seconds=0.0,
            debounce_seconds=0.0,
        )
        big_note = "x" * 1000
        engine = events.EventEngine(
            [rule],
            {"blob": BLOB_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=2_500,  # a couple of big samples' worth
        )
        for t in (1.0, 2.0, 3.0, 4.0, 5.0):
            engine.offer("blob", t, {"accel_x": -1.0, "note": big_note})
        firings = engine.offer("blob", 6.0, {"accel_x": -12.0, "note": big_note})

        assert len(firings) == 1
        window_times = [t for t, _ in firings[0].window]
        assert 1.0 not in window_times
        assert len(window_times) < 6  # never errors, just holds fewer samples than requested


class TestPredicateRuntimeError:
    def test_error_on_one_sample_does_not_break_later_samples(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_evaluate = live.evaluate_predicate
        calls = {"n": 0}

        def fake_evaluate(*args: object, **kwargs: object) -> bool:
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("boom")
            return real_evaluate(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(live, "evaluate_predicate", fake_evaluate)

        rule = _rule()
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        # Call 1, 2: real, no hit. Call 3: forced error on what would be a hit
        # (treated as no-hit, no edge). Call 4: real, hit -> edge -> firing.
        assert engine.offer("imu", 0.0, {"accel_x": -1.0}) == []
        assert engine.offer("imu", 1.0, {"accel_x": -1.0}) == []
        assert engine.offer("imu", 2.0, {"accel_x": -12.0}) == []
        assert engine.counters()["predicate_errors"][rule.name] == 1

        firings = engine.offer("imu", 3.0, {"accel_x": -12.0})
        assert [f.t_event for f in firings] == [3.0]


class TestFlush:
    def test_pending_post_window_fires_on_flush_with_now_as_t_end(self) -> None:
        rule = _rule(post_seconds=5.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        assert engine.offer("imu", 1.0, {"accel_x": -12.0}) == []  # edge, but not released yet

        firings = engine.flush(20.0)
        assert len(firings) == 1
        assert firings[0].t_event == 1.0
        assert firings[0].t_end == 20.0

    def test_flush_still_applies_suppression_gate(self) -> None:
        rule = _rule(post_seconds=5.0)
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=0,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        engine.offer("imu", 1.0, {"accel_x": -12.0})
        firings = engine.flush(20.0)
        assert firings == []
        assert engine.counters()["suppressed"] == 1


class TestCounters:
    def test_counters_shape(self) -> None:
        rule = _rule()
        engine = events.EventEngine(
            [rule],
            {"imu": IMU_STRUCT},
            max_per_minute=100,
            ring_max_samples=1000,
            ring_max_bytes=10_000_000,
        )
        counters = engine.counters()
        assert set(counters) == {"fired", "suppressed", "predicate_errors", "last_event_at"}
        assert counters["fired"] == 0
        assert counters["suppressed"] == 0
        assert counters["predicate_errors"] == {rule.name: 0}
        assert counters["last_event_at"] is None

        engine.offer("imu", 1.0, {"accel_x": -12.0})
        counters = engine.counters()
        assert counters["fired"] == 1
        assert counters["last_event_at"] == 1.0


# -- Task 7: the EventEmitter thread ------------------------------------------------


def _canned_health_inputs() -> HealthInputs:
    """A minimal all-green HealthInputs for emitter tests (the closure seam)."""
    return HealthInputs(
        status={"online": True, "router_alive": True, "heartbeat_alive": True},
        topic_last_seen={},
        cert_expires_at=None,
        enrolled=False,
        disk_free_bytes=10 * 2**30,
        spool_cap_bytes=1_000_000,
        artifacts={},
        artifacts_cap_bytes=1_000_000,
        events_counters={
            "queue_depth": 0,
            "dropped": 0,
            "predicate_errors": 0,
            "fired": 0,
            "suppressed": 0,
        },
        uptime_s=1.0,
    )


class _Clock:
    """An injectable wall clock the test advances by assignment."""

    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _engine(
    rules: list[EventRule], structs: dict | None = None, *, max_per_minute: int = 100
) -> events.EventEngine:
    return events.EventEngine(
        rules,
        structs if structs is not None else {"imu": IMU_STRUCT},
        max_per_minute=max_per_minute,
        ring_max_samples=1000,
        ring_max_bytes=10_000_000,
    )


def _emitter(  # noqa: PLR0913 -- mirrors EventEmitter's own knob-per-collaborator signature
    tmp_path: pathlib.Path,
    rules: list[EventRule],
    structs: dict | None = None,
    artifact_store: object | None = None,
    health_interval_s: float = 3600.0,
    health_settle_s: float = 3600.0,
    now: object = time.time,
    health_inputs: object = _canned_health_inputs,
    max_per_minute: int = 100,
) -> tuple[events.EventEmitter, SampleQueue, Spool]:
    structs = structs if structs is not None else {"imu": IMU_STRUCT}
    queue = SampleQueue(1000)
    spool = Spool(tmp_path / "spool")
    emitter = events.EventEmitter(
        _engine(rules, structs, max_per_minute=max_per_minute),
        queue,
        spool,
        artifact_store=artifact_store,
        health_inputs=health_inputs,
        structs=structs,
        health_interval_s=health_interval_s,
        health_settle_s=health_settle_s,
        now=now,
    )
    return emitter, queue, spool


def _events_lane(spool: Spool) -> list[tuple[int, dict]]:
    return list(spool.pending("events"))


class TestEmitterDrainsAndAppends:
    def test_envelopes_reach_the_events_lane_with_monotonic_seqs(
        self, tmp_path: pathlib.Path
    ) -> None:
        emitter, queue, spool = _emitter(tmp_path, [_rule()])
        emitter.start()
        try:
            queue.put(("imu", 1.0, {"accel_x": -12.0}))  # edge 1
            queue.put(("imu", 2.0, {"accel_x": -1.0}))
            queue.put(("imu", 3.0, {"accel_x": -12.0}))  # edge 2
            assert _wait_until(lambda: len(_events_lane(spool)) == 2)
        finally:
            emitter.stop()

        records = _events_lane(spool)
        assert [seq for seq, _ in records] == [1, 2]
        for (seq, payload), t_start in zip(records, (1.0, 3.0), strict=True):
            assert payload["seq"] == seq
            assert payload["name"] == "hard_decel"
            assert payload["source_topic"] == "imu"
            assert payload["t_start"] == t_start
            assert set(payload) == ENVELOPE_BASE_KEYS

    def test_status_counters_shape(self, tmp_path: pathlib.Path) -> None:
        emitter, _queue, _spool = _emitter(tmp_path, [_rule()])
        emitter.start()
        try:
            counters = emitter.status_counters()
            assert set(counters) == {
                "fired",
                "suppressed",
                "predicate_errors",
                "last_event_at",
                "queue_depth",
                "queue_dropped",
                "spool_failures",
                "health",
                "alive",
                "last_error",
            }
            assert set(counters["health"]) == {"last_report_at", "next_report_at"}
            assert counters["alive"] is True
            assert counters["last_error"] is None
        finally:
            emitter.stop()


class TestEmitterArtifacts:
    def test_artifact_rule_writes_mcap_sharing_the_envelopes_event_id(
        self, tmp_path: pathlib.Path
    ) -> None:
        store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000_000)
        emitter, queue, spool = _emitter(tmp_path, [_rule(artifact="mcap")], artifact_store=store)
        emitter.start()
        try:
            queue.put(("imu", 1.0, {"accel_x": -12.0}))
            assert _wait_until(lambda: len(_events_lane(spool)) == 1)
        finally:
            emitter.stop()

        payload = _events_lane(spool)[0][1]
        assert set(payload) == ENVELOPE_BASE_KEYS | {"artifact"}
        uri = payload["artifact"]["uri"]
        assert uri.startswith("file://")
        path = pathlib.Path(uri.removeprefix("file://"))
        assert path.exists()
        assert path.name == f"hard_decel-{payload['event_id']}.mcap"
        assert "artifact_error" not in payload["summary"]

    def test_store_returning_none_sets_budget_error_and_no_artifact_key(
        self, tmp_path: pathlib.Path
    ) -> None:
        store = ArtifactStore(tmp_path / "artifacts", max_bytes=1)  # everything over budget
        emitter, queue, spool = _emitter(tmp_path, [_rule(artifact="mcap")], artifact_store=store)
        emitter.start()
        try:
            queue.put(("imu", 1.0, {"accel_x": -12.0}))
            assert _wait_until(lambda: len(_events_lane(spool)) == 1)
        finally:
            emitter.stop()

        payload = _events_lane(spool)[0][1]
        assert set(payload) == ENVELOPE_BASE_KEYS  # the event ALWAYS goes out
        assert payload["summary"]["artifact_error"] == "artifact byte budget exceeded"

    def test_store_raising_sets_the_error_string_and_event_still_goes_out(
        self, tmp_path: pathlib.Path
    ) -> None:
        class _RaisingStore:
            def store(self, *args: object, **kwargs: object) -> pathlib.Path | None:
                raise ValueError("mcap writer exploded")

        emitter, queue, spool = _emitter(
            tmp_path, [_rule(artifact="mcap")], artifact_store=_RaisingStore()
        )
        emitter.start()
        try:
            queue.put(("imu", 1.0, {"accel_x": -12.0}))
            assert _wait_until(lambda: len(_events_lane(spool)) == 1)
            assert emitter.alive  # a store failure never kills the thread
        finally:
            emitter.stop()

        payload = _events_lane(spool)[0][1]
        assert set(payload) == ENVELOPE_BASE_KEYS
        assert payload["summary"]["artifact_error"] == "mcap writer exploded"

    def test_emit_sorts_an_unsorted_window_by_t_before_store(self, tmp_path: pathlib.Path) -> None:
        """Task-5 F3: out-of-order sources produce unsorted ring windows; MCAP
        readers assume chronological log time, so `_emit` sorts before storing."""

        class _RecordingStore:
            def __init__(self) -> None:
                self.calls: list[list[tuple[float, dict]]] = []

            def store(self, *args: object, **kwargs: object) -> pathlib.Path | None:
                self.calls.append(args[4])  # the samples list
                return None

        store = _RecordingStore()
        rule = _rule(pre_seconds=10.0, artifact="mcap")
        emitter, queue, spool = _emitter(tmp_path, [rule], artifact_store=store)
        emitter.start()
        try:
            queue.put(("imu", 2.0, {"accel_x": -1.0}))
            queue.put(("imu", 1.0, {"accel_x": -1.0}))  # out of order
            queue.put(("imu", 3.0, {"accel_x": -12.0}))  # edge, fires now
            assert _wait_until(lambda: bool(store.calls))
        finally:
            emitter.stop()

        assert [t for t, _ in store.calls[0]] == [1.0, 2.0, 3.0]


class TestEmitterStopAndFailures:
    def test_stop_fires_a_post_window_pending_event(self, tmp_path: pathlib.Path) -> None:
        emitter, queue, spool = _emitter(tmp_path, [_rule(post_seconds=5.0)])
        emitter.start()
        try:
            queue.put(("imu", 1.0, {"accel_x": -12.0}))  # edge; post window never elapses
            assert _wait_until(lambda: emitter.status_counters()["queue_depth"] == 0)
            assert _events_lane(spool) == []
        finally:
            emitter.stop()  # final flush releases the pending edge, best-effort

        records = _events_lane(spool)
        assert len(records) == 1
        assert records[0][1]["t_start"] == 1.0

    def test_spool_append_failure_counts_and_thread_survives(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        emitter, queue, spool = _emitter(tmp_path, [_rule()])

        def boom(*args: object, **kwargs: object) -> None:
            raise SpoolFullError("disk full")

        monkeypatch.setattr(spool, "append_next", boom)
        emitter.start()
        try:
            queue.put(("imu", 1.0, {"accel_x": -12.0}))
            assert _wait_until(lambda: emitter.status_counters()["spool_failures"] == 1)
            assert emitter.alive
        finally:
            monkeypatch.undo()
            emitter.stop()

    def test_fatal_tick_error_is_visible_via_alive_and_last_error(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        emitter, queue, _spool = _emitter(tmp_path, [_rule()])

        def boom(*args: object, **kwargs: object) -> list:
            raise RuntimeError("engine exploded")

        monkeypatch.setattr(emitter._engine, "offer", boom)
        emitter.start()
        queue.put(("imu", 1.0, {"accel_x": -12.0}))
        assert _wait_until(lambda: not emitter.is_alive())
        assert emitter.alive is False
        assert emitter.last_error is not None
        assert "engine exploded" in emitter.last_error
        emitter.stop()  # must not raise (no final flush after a fatal error)

    def test_final_flush_engine_error_is_caught_and_recorded_as_last_error(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rider (c): `_final_flush`'s broad try/except -- a failure INSIDE
        `engine.flush` (the final-flush-only call path, distinct from the
        `offer` fatal-tick path above) must not propagate out of `stop()`;
        it's logged and recorded via `last_error` instead."""
        emitter, queue, _spool = _emitter(tmp_path, [_rule()])
        emitter.start()
        try:

            def boom(*args: object, **kwargs: object) -> list:
                raise RuntimeError("flush exploded")

            monkeypatch.setattr(emitter._engine, "flush", boom)
        finally:
            emitter.stop()  # must not raise

        assert emitter.last_error is not None
        assert "flush exploded" in emitter.last_error


class TestEmitterSuppressedCountSurvivesAppendFailure:
    """End-to-end version of `TestSuppressedCountPreservedUntilAck`: a real
    spool append failure must not lose a firing's suppressed count."""

    def test_append_failure_preserves_suppressed_count_for_next_success(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rule = _rule(post_seconds=0.0, debounce_seconds=0.0)
        emitter, queue, spool = _emitter(tmp_path, [rule], max_per_minute=1)

        real_append_next = spool.append_next
        calls = {"n": 0}

        def flaky_append_next(lane: str, build: object) -> object:
            calls["n"] += 1
            if calls["n"] == 2:  # the SECOND append (the one carrying suppressed=1)
                raise SpoolFullError("disk full")
            return real_append_next(lane, build)

        monkeypatch.setattr(spool, "append_next", flaky_append_next)

        emitter.start()
        try:
            queue.put(("imu", 1.0, {"accel_x": -12.0}))  # Firing A: fires, suppressed=0
            queue.put(("imu", 2.0, {"accel_x": -1.0}))
            queue.put(("imu", 3.0, {"accel_x": -12.0}))  # gated -> suppressed_since_last=1
            queue.put(("imu", 4.0, {"accel_x": -1.0}))
            queue.put(("imu", 65.0, {"accel_x": -12.0}))  # Firing B: releases, append FAILS
            queue.put(("imu", 66.0, {"accel_x": -1.0}))
            assert _wait_until(lambda: emitter.status_counters()["spool_failures"] == 1)

            queue.put(("imu", 127.0, {"accel_x": -12.0}))  # Firing C: must still carry it
            queue.put(("imu", 128.0, {"accel_x": -1.0}))
            assert _wait_until(lambda: len(_events_lane(spool)) == 2)  # A and C only
        finally:
            monkeypatch.undo()
            emitter.stop()

        payloads = [p for _, p in _events_lane(spool)]
        assert payloads[0]["summary"].get("suppressed", 0) == 0  # Firing A
        assert payloads[1]["summary"]["suppressed"] == 1  # Firing C carries B's lost count


class TestEmitterJoinTimeout:
    def test_stop_joins_with_a_12s_timeout(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rider (d): aligned with `StreamRouter.stop`'s own 12s join bound
        (Codex review, 3906982943) -- both threads can be blocked inside the
        same `MqttPublisher.publish`'s 10s QoS-1 `wait_for_publish` wait, so
        the join must outlast that."""
        emitter, _queue, _spool = _emitter(tmp_path, [_rule()])

        join_calls: list[float | None] = []
        real_join = emitter.join

        def spy_join(timeout: float | None = None) -> None:
            join_calls.append(timeout)
            real_join(timeout)

        monkeypatch.setattr(emitter, "join", spy_join)

        emitter.start()
        emitter.stop()

        assert join_calls
        assert join_calls[0] is not None and join_calls[0] > 10.0


class TestEmitterHealthSchedule:
    def test_settle_then_interval_schedule_with_an_injected_clock(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", None)
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        clock = _Clock(1000.0)
        emitter, _queue, spool = _emitter(
            tmp_path, [], structs={}, health_settle_s=0.0, health_interval_s=3600.0, now=clock
        )
        emitter.start()
        try:
            assert _wait_until(lambda: len(_events_lane(spool)) == 1)
            time.sleep(0.3)  # more ticks elapse; the schedule must not re-fire
            assert len(_events_lane(spool)) == 1

            clock.t = 1000.0 + 3600.0 + 1.0
            assert _wait_until(lambda: len(_events_lane(spool)) == 2)
        finally:
            emitter.stop()

        first, second = (payload for _, payload in _events_lane(spool))
        for payload in (first, second):
            assert payload["name"] == "health_report"
            assert payload["source_topic"] == "internal:health"
            assert set(payload) == ENVELOPE_BASE_KEYS
            assert set(payload["summary"]) == {"schema_rev", "source", "checks", "verdict"}
            uuid.UUID(payload["event_id"])
        assert first["t_start"] == 1000.0  # runtime start
        assert first["t_end"] == 1000.0
        assert second["t_start"] == first["t_end"]  # reports describe an interval
        assert second["t_end"] == 4601.0

    def test_summary_carries_build_when_provenance_set(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", "abc123")
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        clock = _Clock(1000.0)
        emitter, _queue, spool = _emitter(tmp_path, [], structs={}, health_settle_s=0.0, now=clock)
        emitter.start()
        try:
            assert _wait_until(lambda: len(_events_lane(spool)) == 1)
        finally:
            emitter.stop()
        summary = _events_lane(spool)[0][1]["summary"]
        assert set(summary) == {"schema_rev", "source", "checks", "verdict", "build"}
        assert summary["build"] == {"build_id": "abc123"}

    def test_failing_inputs_gatherer_skips_the_report_and_retries_next_interval(
        self, tmp_path: pathlib.Path
    ) -> None:
        state = {"fail": True}

        def flaky_inputs() -> HealthInputs:
            if state["fail"]:
                raise RuntimeError("gatherer exploded")
            return _canned_health_inputs()

        clock = _Clock(0.0)
        emitter, _queue, spool = _emitter(
            tmp_path,
            [],
            structs={},
            health_settle_s=0.0,
            health_interval_s=100.0,
            now=clock,
            health_inputs=flaky_inputs,
        )
        emitter.start()
        try:
            assert _wait_until(lambda: emitter.status_counters()["last_error"] is not None)
            assert emitter.alive  # never kills the thread
            assert _events_lane(spool) == []  # report skipped, not half-written

            state["fail"] = False
            clock.t = 101.0  # retried at the NEXT interval, not rescheduled early
            assert _wait_until(lambda: len(_events_lane(spool)) == 1)
            assert emitter.status_counters()["last_error"] is None
        finally:
            emitter.stop()

    def test_spool_append_failure_keeps_old_baseline_so_next_report_spans_full_period(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review (P2, events.py:454): `_emit_health` used to commit
        `_health_prev`/`_last_report_at` before the append was even
        attempted. If that append then failed (spool full, transient disk
        error -- `_append_to_events_lane` swallows it), the NEXT delivered
        report computed its deltas from the undelivered report's snapshot
        and `t_start` skipped ahead too -- the missing interval's queue
        drops/evictions/reconnects were never reported. The baseline must
        only advance on a successful append, so the next report's
        `t_start`/`t_end` span BOTH periods."""
        monkeypatch.setattr(settings, "BAGEL_BUILD_ID", None)
        monkeypatch.setattr(settings, "BAGEL_VCS_REF", None)
        # health_settle_s > 0 so the FIRST (failing) report's `now` (1050.0)
        # differs from `_started_at` (1000.0) -- otherwise a buggy commit-
        # on-failure would coincidentally produce the same t_start as the
        # fix, and the test couldn't tell them apart.
        clock = _Clock(1000.0)
        emitter, _queue, spool = _emitter(
            tmp_path, [], structs={}, health_settle_s=50.0, health_interval_s=100.0, now=clock
        )

        real_append_next = spool.append_next
        calls = {"n": 0}

        def flaky_append_next(lane: str, build: object) -> object:
            calls["n"] += 1
            if calls["n"] == 1:  # the FIRST scheduled report's append fails
                raise SpoolFullError("disk full")
            return real_append_next(lane, build)

        monkeypatch.setattr(spool, "append_next", flaky_append_next)

        emitter.start()
        try:
            clock.t = 1050.0  # first scheduled report; its append fails
            assert _wait_until(lambda: emitter.status_counters()["spool_failures"] == 1)
            assert _events_lane(spool) == []  # the failed report was never recorded

            clock.t = 1150.0 + 1.0  # the next scheduled tick (settle + 2 intervals)
            assert _wait_until(lambda: len(_events_lane(spool)) == 1)
        finally:
            monkeypatch.undo()
            emitter.stop()

        payload = _events_lane(spool)[0][1]
        # t_start is still the runtime start (1000.0) -- NOT the failed
        # report's own `now` (1050.0) -- because `_last_report_at` was
        # never committed on failure, so this report's period covers the
        # full span since start, not just since the failed attempt.
        assert payload["t_start"] == 1000.0
        assert payload["t_end"] == 1151.0
