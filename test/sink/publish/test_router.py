"""SampleQueue: bounded, drop-oldest, counted (spec §4).

RouterCore: extraction, rate-capping, batch-building (spec §2/§4).
"""

import importlib
import sys

import pyarrow as pa
import pytest

from src.message.base import AccessPath
from src.sink.publish.config import ResolvedChannel
from src.sink.publish.router import RouterCore, SampleQueue, extract_value


class TestSampleQueue:
    def test_put_and_drain_fifo(self) -> None:
        q = SampleQueue(maxsize=10)
        for i in range(3):
            q.put(("/imu", float(i), {"n": i}))
        got = q.drain(max_items=10, timeout_s=0.01)
        assert [s[1] for s in got] == [0.0, 1.0, 2.0]
        assert q.depth == 0

    def test_overflow_drops_oldest_and_counts(self) -> None:
        q = SampleQueue(maxsize=3)
        for i in range(5):
            q.put(("/imu", float(i), {}))
        assert q.dropped == 2
        got = q.drain(max_items=10, timeout_s=0.01)
        assert [s[1] for s in got] == [2.0, 3.0, 4.0]

    def test_drain_respects_max_items(self) -> None:
        q = SampleQueue(maxsize=10)
        for i in range(6):
            q.put(("/t", float(i), {}))
        got = q.drain(max_items=4, timeout_s=0.01)
        assert len(got) == 4 and q.depth == 2

    def test_drain_times_out_empty(self) -> None:
        q = SampleQueue(maxsize=4)
        assert q.drain(max_items=4, timeout_s=0.01) == []

    def test_as_tap_feeds_queue(self) -> None:
        q = SampleQueue(maxsize=4)
        tap = q.as_tap()
        tap("/imu", 1.0, {"x": 1})
        assert q.depth == 1


def test_router_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.router", raising=False)
    importlib.import_module("src.sink.publish.router")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)


def _chan(
    name: str, topic: str, path: list[str], rate_hz: float, type_: str = "number"
) -> ResolvedChannel:
    return ResolvedChannel(
        name=name,
        type=type_,
        source_topic=topic,
        source_field=".".join(path),
        rate_hz=rate_hz,
        paths={"value": AccessPath(path=path, pa_type=pa.float64())},
    )


class TestExtractValue:
    def test_nested(self) -> None:
        assert extract_value({"a": {"b": 3.5}}, ["a", "b"]) == 3.5

    def test_missing_raises_keyerror(self) -> None:
        import pytest

        with pytest.raises(KeyError):
            extract_value({"a": {}}, ["a", "b"])


class TestRateCapAndBatch:
    def test_last_sample_per_interval_wins(self) -> None:
        core = RouterCore([_chan("imu.x", "/imu", ["x"], rate_hz=1.0)], flush_interval_s=1.0)
        # three samples inside the same 1 Hz slot: last wins
        core.offer("/imu", 10.1, {"x": 1.0})
        core.offer("/imu", 10.5, {"x": 2.0})
        core.offer("/imu", 10.9, {"x": 3.0})
        # next slot
        core.offer("/imu", 11.2, {"x": 4.0})
        batch = core.flush(t_batch=12.0)
        assert [(s["t"], s["v"]) for s in batch["samples"]] == [(10.9, 3.0), (11.2, 4.0)]
        assert batch["v"] == 1 and "seq" not in batch

    def test_channels_from_other_topics_ignored(self) -> None:
        core = RouterCore([_chan("imu.x", "/imu", ["x"], 5.0)], flush_interval_s=1.0)
        core.offer("/other", 1.0, {"x": 9.9})
        assert core.flush(2.0) is None

    def test_missing_field_skipped_and_counted(self) -> None:
        core = RouterCore([_chan("imu.x", "/imu", ["x"], 5.0)], flush_interval_s=1.0)
        core.offer("/imu", 1.0, {"y": 1.0})
        assert core.skipped == 1 and core.flush(2.0) is None

    def test_geo_channel_builds_object(self) -> None:
        chan = ResolvedChannel(
            name="odom.geo",
            type="geo",
            source_topic="/odom",
            source_field="lat=a,lon=b",
            rate_hz=1.0,
            paths={
                "lat": AccessPath(path=["a"], pa_type=pa.float64()),
                "lon": AccessPath(path=["b"], pa_type=pa.float64()),
            },
        )
        core = RouterCore([chan], flush_interval_s=1.0)
        core.offer("/odom", 3.0, {"a": 37.1, "b": -122.0})
        (sample,) = core.flush(4.0)["samples"]
        assert sample["c"] == "odom.geo" and sample["v"] == {"lat": 37.1, "lon": -122.0}

    def test_flush_empties_and_orders_across_channels(self) -> None:
        core = RouterCore(
            [_chan("a.x", "/a", ["x"], 10.0), _chan("b.x", "/b", ["x"], 10.0)],
            flush_interval_s=1.0,
        )
        core.offer("/b", 2.0, {"x": 2.0})
        core.offer("/a", 1.0, {"x": 1.0})
        batch = core.flush(3.0)
        assert [s["t"] for s in batch["samples"]] == [1.0, 2.0]
        assert core.flush(4.0) is None

    def test_should_flush_on_size_or_interval(self) -> None:
        core = RouterCore([_chan("a.x", "/a", ["x"], 50.0)], flush_interval_s=1.0, max_samples=2)
        core.flush(0.0)  # set _last_flush
        assert not core.should_flush(now=0.5, pending_count=1)
        assert core.should_flush(now=0.5, pending_count=2)
        assert core.should_flush(now=1.6, pending_count=0)

    def test_late_sample_for_already_emitted_slot_is_dropped(self) -> None:
        # Slot-survival: once a (channel, slot) has been popped by flush(), a
        # later-arriving offer() for that same slot -- or an earlier one --
        # must not resurrect it into a future batch. Without this guard, a
        # reordered/late sample could re-emit stale data for a slot the
        # subscriber already received.
        core = RouterCore([_chan("imu.x", "/imu", ["x"], rate_hz=1.0)], flush_interval_s=1.0)
        core.offer("/imu", 10.9, {"x": 3.0})  # slot 10
        batch = core.flush(t_batch=11.0)
        assert [(s["t"], s["v"]) for s in batch["samples"]] == [(10.9, 3.0)]

        # A late sample lands for the same (already-emitted) slot 10.
        core.offer("/imu", 10.95, {"x": 99.0})
        assert core.pending_count == 0
        assert core.flush(t_batch=12.0) is None

        # The next slot still works normally afterward.
        core.offer("/imu", 11.4, {"x": 5.0})
        batch2 = core.flush(t_batch=13.0)
        assert [(s["t"], s["v"]) for s in batch2["samples"]] == [(11.4, 5.0)]
