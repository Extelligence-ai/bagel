"""Tests for the pure fleet event engine: predicates, windows, debounce,
suppression, ring bounds and the binding event envelope (fleet step 8, Task 5).

All pure -- synthetic structs via `pa.struct`, no threads, clock-injected via
the `t`/`now` arguments the engine's methods take.
"""

import importlib
import sys
import uuid

import pyarrow as pa
import pytest

from settings import settings
from src.pipeline import live
from src.sink.publish import StreamConfigError, events
from src.sink.publish.config import EventRule

IMU_STRUCT = pa.struct([("accel_x", pa.float64())])
BLOB_STRUCT = pa.struct([("accel_x", pa.float64()), ("note", pa.string())])


def _rule(  # noqa: PLR0913 -- one field per EventRule window/predicate knob, kept explicit
    name: str = "hard_decel",
    topic: str = "imu",
    predicate: str = "imu['accel_x'] < -10",
    pre_seconds: float = 0.0,
    post_seconds: float = 0.0,
    debounce_seconds: float = 0.0,
) -> EventRule:
    return EventRule(
        name=name,
        topic=topic,
        predicate=predicate,
        pre_seconds=pre_seconds,
        post_seconds=post_seconds,
        debounce_seconds=debounce_seconds,
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
        assert set(payload) == {
            "v",
            "seq",
            "event_id",
            "name",
            "t_start",
            "t_end",
            "source_topic",
            "summary",
        }

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
