"""Heartbeat payload building and HeartbeatThread lifecycle (fleet streaming spec §3)."""

import importlib
import pathlib
import sys
import time

import pytest

from src.sink.publish import heartbeat as heartbeat_mod
from src.sink.publish.heartbeat import HEARTBEAT_INTERVAL_S, HeartbeatThread, build_heartbeat
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.spool import Spool


def test_heartbeat_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.heartbeat", raising=False)
    importlib.import_module("src.sink.publish.heartbeat")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)


class TestBuildHeartbeat:
    def test_golden_shape_and_exact_keys(self) -> None:
        payload = build_heartbeat(
            started_at=100.0,
            subscriptions=["/imu", "/odom"],
            channels_active=3,
            queue_depth=5,
            queue_dropped=2,
            spool_stats={
                "channels": {"bytes": 10, "pending": 1, "evicted": 0},
                "events": {"bytes": 20, "pending": 2, "evicted": 1},
                "heartbeat": {"bytes": 30, "pending": 0, "evicted": 0},
            },
            disk_free_bytes=1_000_000,
            reconnects=4,
            now=130.0,
        )
        assert payload == {
            "v": 1,
            "t": 130.0,
            "online": True,
            "bagel_version": payload["bagel_version"],  # asserted separately below
            "uptime_s": 30.0,
            "subscriptions": ["/imu", "/odom"],
            "channels_active": 3,
            "queue": {"depth": 5, "dropped": 2},
            "spool": {"bytes": 60, "pending": 3, "evicted": 1},
            "disk_free_bytes": 1_000_000,
            "cert_expires_at": None,
            "reconnects": 4,
        }
        assert isinstance(payload["bagel_version"], str) and payload["bagel_version"]
        assert set(payload) == {
            "v",
            "t",
            "online",
            "bagel_version",
            "uptime_s",
            "subscriptions",
            "channels_active",
            "queue",
            "spool",
            "disk_free_bytes",
            "cert_expires_at",
            "reconnects",
        }

    def test_now_defaults_to_wall_clock(self) -> None:
        before = time.time()
        payload = build_heartbeat(
            started_at=0.0,
            subscriptions=[],
            channels_active=0,
            queue_depth=0,
            queue_dropped=0,
            spool_stats={},
            disk_free_bytes=0,
            reconnects=0,
        )
        after = time.time()
        assert before <= payload["t"] <= after

    def test_empty_spool_stats_aggregate_to_zero(self) -> None:
        payload = build_heartbeat(
            started_at=0.0,
            subscriptions=[],
            channels_active=0,
            queue_depth=0,
            queue_dropped=0,
            spool_stats={},
            disk_free_bytes=0,
            reconnects=0,
            now=1.0,
        )
        assert payload["spool"] == {"bytes": 0, "pending": 0, "evicted": 0}

    def test_accepts_lanestats_like_objects_with_attributes(self) -> None:
        class FakeLaneStats:
            def __init__(self, bytes_: int, pending: int, evicted: int) -> None:
                self.bytes = bytes_
                self.pending = pending
                self.evicted = evicted

        payload = build_heartbeat(
            started_at=0.0,
            subscriptions=[],
            channels_active=0,
            queue_depth=0,
            queue_dropped=0,
            spool_stats={"channels": FakeLaneStats(5, 1, 0)},
            disk_free_bytes=0,
            reconnects=0,
            now=1.0,
        )
        assert payload["spool"] == {"bytes": 5, "pending": 1, "evicted": 0}


class TestBagelVersion:
    def test_returns_nonempty_string(self) -> None:
        assert isinstance(heartbeat_mod.bagel_version(), str)
        assert heartbeat_mod.bagel_version()

    def test_falls_back_when_distribution_metadata_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import importlib.metadata

        def raise_not_found(_name: str) -> str:
            raise importlib.metadata.PackageNotFoundError("bagel")

        monkeypatch.setattr(heartbeat_mod.importlib.metadata, "version", raise_not_found)
        version = heartbeat_mod.bagel_version()
        assert isinstance(version, str) and version != "unknown"

    def test_returns_unknown_when_pyproject_also_unreadable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import importlib.metadata

        def raise_not_found(_name: str) -> str:
            raise importlib.metadata.PackageNotFoundError("bagel")

        def raise_os_error() -> str:
            raise OSError("nope")

        monkeypatch.setattr(heartbeat_mod.importlib.metadata, "version", raise_not_found)
        monkeypatch.setattr(heartbeat_mod, "_pyproject_version", raise_os_error)
        assert heartbeat_mod.bagel_version() == "unknown"


class TestDiskFree:
    def test_matches_shutil_disk_usage(self, tmp_path: pathlib.Path) -> None:
        import shutil

        # Free space can shift between the two calls on a live filesystem
        # (other processes writing concurrently); assert same order of
        # magnitude rather than bit-for-bit equality to avoid flakes.
        before = shutil.disk_usage(tmp_path).free
        got = heartbeat_mod.disk_free(tmp_path)
        after = shutil.disk_usage(tmp_path).free
        assert isinstance(got, int)
        assert min(before, after) * 0.9 <= got <= max(before, after) * 1.1


class FakePublisher(Publisher):
    """In-memory Publisher double: records heartbeat publishes, can be told to fail."""

    def __init__(self) -> None:
        self.heartbeat_calls: list[dict] = []
        self._fail_next = 0
        self._connected = True

    def connect(self) -> None:
        self._connected = True

    def publish(
        self, kind: str, payload: dict, *, retain: bool = False, timeout_s: float = 10.0
    ) -> None:
        assert kind == "heartbeat"
        if self._fail_next > 0:
            self._fail_next -= 1
            raise PublishError("heartbeat publish failed")
        self.heartbeat_calls.append(payload)

    def close(self) -> None:
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def fail_next(self, n: int) -> None:
        self._fail_next = n


class TestHeartbeatThread:
    def test_ticks_publish_heartbeat_payloads(self, tmp_path: pathlib.Path) -> None:
        pub = FakePublisher()
        calls = []

        def factory() -> dict:
            calls.append(1)
            return {"v": 1, "n": len(calls)}

        thread = HeartbeatThread(pub, factory, interval_s=0.01)
        thread.start()
        deadline = time.monotonic() + 2.0
        while len(pub.heartbeat_calls) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        thread.stop()
        assert len(pub.heartbeat_calls) >= 3
        assert not thread.is_alive()

    def test_publish_failure_spools_to_heartbeat_lane(self, tmp_path: pathlib.Path) -> None:
        pub = FakePublisher()
        pub.fail_next(1)
        spool = Spool(tmp_path / "spool")
        payload = {"v": 1, "marker": "spooled"}
        thread = HeartbeatThread(pub, lambda: payload, interval_s=10.0, spool=spool)

        thread._tick()  # drive synchronously; no sleeping needed

        assert pub.heartbeat_calls == []
        pending = list(spool.pending("heartbeat"))
        assert len(pending) == 1
        assert pending[0][1] == payload

    def test_publish_failure_without_spool_swallowed(self) -> None:
        pub = FakePublisher()
        pub.fail_next(1)
        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=10.0)
        thread._tick()  # must not raise
        assert pub.heartbeat_calls == []

    def test_stop_joins_promptly(self) -> None:
        pub = FakePublisher()
        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=HEARTBEAT_INTERVAL_S)
        thread.start()
        time.sleep(0.02)

        start = time.monotonic()
        thread.stop()
        elapsed = time.monotonic() - start

        assert not thread.is_alive()
        assert elapsed < 2.0

    def test_module_constant_is_thirty_seconds(self) -> None:
        assert HEARTBEAT_INTERVAL_S == 30.0
