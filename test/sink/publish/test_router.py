"""SampleQueue: bounded, drop-oldest, counted (spec §4)."""

import importlib
import sys

import pytest

from src.sink.publish.router import SampleQueue


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
