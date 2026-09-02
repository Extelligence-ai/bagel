"""Heartbeat payload building and HeartbeatThread lifecycle (fleet streaming spec §3)."""

import importlib
import logging
import pathlib
import sys
import time

import pytest

from src.sink.publish import heartbeat as heartbeat_mod
from src.sink.publish.heartbeat import HEARTBEAT_INTERVAL_S, HeartbeatThread, build_heartbeat
from src.sink.publish.publisher import Publisher, PublishError
from src.sink.publish.spool import Spool, SpoolFullError


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

    def test_cert_expires_at_defaults_to_none(self) -> None:
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
        assert payload["cert_expires_at"] is None

    def test_cert_expires_at_passed_through_when_given(self) -> None:
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
            cert_expires_at="2027-01-01T00:00:00Z",
        )
        assert payload["cert_expires_at"] == "2027-01-01T00:00:00Z"

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


class TestPyprojectVersionRegexFallback:
    """CI P0 (Codex review): heartbeat.py's module-level `import tomllib` broke the
    ros2-humble/iron Docker images (Python 3.10 -- tomllib is stdlib only on
    3.11+), redding out CI's image import-probe on PR 210. `_pyproject_version()`
    now parses `[project].version` with a zero-dependency regex instead.
    """

    def test_parses_version_via_regex_zero_deps(self, tmp_path: pathlib.Path) -> None:
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "bagel"\nversion = "9.9.9"\ndescription = "x"\n'
        )
        assert heartbeat_mod._pyproject_version(root=tmp_path) == "9.9.9"

    def test_finds_the_version_line_among_surrounding_content(
        self, tmp_path: pathlib.Path
    ) -> None:
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "bagel"\nversion = "2.3.0"\n\n[tool.other]\nfoo = "bar"\n'
        )
        assert heartbeat_mod._pyproject_version(root=tmp_path) == "2.3.0"

    def test_raises_when_no_version_line_present(self, tmp_path: pathlib.Path) -> None:
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "bagel"\n')
        with pytest.raises(Exception):  # noqa: B017 -- bagel_version() catches broadly
            heartbeat_mod._pyproject_version(root=tmp_path)

    def test_raises_when_pyproject_missing(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(Exception):  # noqa: B017 -- bagel_version() catches broadly
            heartbeat_mod._pyproject_version(root=tmp_path)

    def test_bagel_version_falls_back_to_regex_parsed_pyproject_version(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import importlib.metadata

        (tmp_path / "pyproject.toml").write_text('[project]\nversion = "7.7.7"\n')
        real_pyproject_version = heartbeat_mod._pyproject_version

        def raise_not_found(_name: str) -> str:
            raise importlib.metadata.PackageNotFoundError("bagel")

        monkeypatch.setattr(heartbeat_mod.importlib.metadata, "version", raise_not_found)
        monkeypatch.setattr(
            heartbeat_mod, "_pyproject_version", lambda: real_pyproject_version(tmp_path)
        )

        assert heartbeat_mod.bagel_version() == "7.7.7"

    def test_bagel_version_returns_unknown_when_pyproject_has_no_version_line(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import importlib.metadata

        (tmp_path / "pyproject.toml").write_text('[project]\nname = "bagel"\n')  # no version
        real_pyproject_version = heartbeat_mod._pyproject_version

        def raise_not_found(_name: str) -> str:
            raise importlib.metadata.PackageNotFoundError("bagel")

        monkeypatch.setattr(heartbeat_mod.importlib.metadata, "version", raise_not_found)
        monkeypatch.setattr(
            heartbeat_mod, "_pyproject_version", lambda: real_pyproject_version(tmp_path)
        )

        assert heartbeat_mod.bagel_version() == "unknown"

    def test_tomllib_is_not_imported_by_this_module(self) -> None:
        # The whole point: tomllib is 3.11+ stdlib only, and this module must
        # import cleanly on the 3.10-based ros2-humble/iron images.
        assert not hasattr(heartbeat_mod, "tomllib")

    def test_heartbeat_module_does_not_import_tomllib_eagerly(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for name in [m for m in sys.modules if m == "tomllib" or m.startswith("tomllib.")]:
            monkeypatch.delitem(sys.modules, name)
        monkeypatch.delitem(sys.modules, "src.sink.publish.heartbeat", raising=False)
        importlib.import_module("src.sink.publish.heartbeat")
        assert not any(m == "tomllib" or m.startswith("tomllib.") for m in sys.modules)


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
        assert thread.spool_failures == 0  # the spool write itself succeeded

    def test_publish_failure_without_spool_swallowed(self) -> None:
        pub = FakePublisher()
        pub.fail_next(1)
        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=10.0)
        thread._tick()  # must not raise
        assert pub.heartbeat_calls == []
        assert thread.spool_failures == 0  # no spool configured; nothing to fail

    def test_spool_write_failure_after_publish_failure_logs_warning_and_counts(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # PublishError (broker offline) is normal and stays DEBUG-quiet (see
        # test_publish_failure_spools_to_heartbeat_lane above); a failure
        # writing to the never-drop "heartbeat" lane afterward is not --
        # spec §4 requires it be visible, not silently dropped.
        pub = FakePublisher()
        pub.fail_next(1000)  # every tick's publish fails

        class BoomSpool:
            """Duck-typed Spool stand-in: next_seq works, append always fails."""

            def next_seq(self, lane: str) -> int:
                return 1

            def append(self, lane: str, seq: int, payload: dict) -> None:
                raise SpoolFullError(f"lane '{lane}': disk full")

        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=10.0, spool=BoomSpool())

        with caplog.at_level(logging.WARNING):
            thread.start()
            deadline = time.monotonic() + 2.0
            while thread.spool_failures < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            assert thread.is_alive()  # the failure never escaped run()'s loop
            thread.stop()

        assert thread.spool_failures == 1
        assert pub.heartbeat_calls == []
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings
        assert any("heartbeat" in r.getMessage() for r in warnings)

    def test_factory_failure_skips_beat_logs_warning_and_stays_alive(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        pub = FakePublisher()

        def boom() -> dict:
            raise RuntimeError("spool corrupt")

        thread = HeartbeatThread(pub, boom, interval_s=0.01)

        with caplog.at_level(logging.WARNING):
            thread.start()
            deadline = time.monotonic() + 2.0
            while thread.last_error is None and time.monotonic() < deadline:
                time.sleep(0.01)
            assert thread.alive is True  # the factory failure never escaped run()'s loop
            thread.stop()

        assert pub.heartbeat_calls == []
        assert thread.alive is False  # stopped and joined
        assert thread.last_error is not None
        assert "spool corrupt" in thread.last_error
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings
        assert any("payload factory" in r.getMessage() for r in warnings)

    def test_factory_failure_synchronous_tick_skips_and_sets_last_error(self) -> None:
        pub = FakePublisher()

        def boom() -> dict:
            raise RuntimeError("spool corrupt")

        thread = HeartbeatThread(pub, boom, interval_s=HEARTBEAT_INTERVAL_S)

        thread._tick()  # drive synchronously; no sleeping needed

        assert pub.heartbeat_calls == []
        assert thread.last_error is not None
        assert "spool corrupt" in thread.last_error

    def test_factory_recovery_clears_last_error(self) -> None:
        pub = FakePublisher()
        calls = {"n": 0}

        def flaky() -> dict:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("disk missing")
            return {"v": 1}

        thread = HeartbeatThread(pub, flaky, interval_s=HEARTBEAT_INTERVAL_S)

        thread._tick()
        assert thread.last_error is not None

        thread._tick()
        assert thread.last_error is None
        assert pub.heartbeat_calls == [{"v": 1}]

    def test_thread_alive_running(self) -> None:
        pub = FakePublisher()
        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=0.01)
        thread.start()
        try:
            deadline = time.monotonic() + 2.0
            while not thread.alive and time.monotonic() < deadline:
                time.sleep(0.01)
            assert thread.alive is True
        finally:
            thread.stop()
        assert thread.alive is False

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


class TestRenewalCheckHook:
    def test_synchronous_tick_invokes_renewal_check(self) -> None:
        pub = FakePublisher()
        calls = []
        thread = HeartbeatThread(
            pub, lambda: {"v": 1}, interval_s=10.0, renewal_check=lambda: calls.append(1)
        )

        thread._tick()

        assert calls == [1]
        assert pub.heartbeat_calls == [{"v": 1}]

    def test_no_renewal_check_is_a_safe_default(self) -> None:
        pub = FakePublisher()
        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=10.0)

        thread._tick()  # must not raise -- renewal_check defaults to None

        assert pub.heartbeat_calls == [{"v": 1}]

    def test_renewal_check_exception_is_swallowed_and_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        pub = FakePublisher()

        def boom() -> None:
            raise RuntimeError("renewal exploded")

        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=10.0, renewal_check=boom)

        with caplog.at_level(logging.WARNING):
            thread._tick()  # must not raise

        # The payload still gets built and published -- the hook's failure
        # has no bearing on the rest of this tick.
        assert pub.heartbeat_calls == [{"v": 1}]
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("renewal_check" in r.getMessage() for r in warnings)

    def test_renewal_check_exception_never_kills_the_thread(self) -> None:
        pub = FakePublisher()
        ticks = {"n": 0}

        def boom() -> None:
            ticks["n"] += 1
            raise RuntimeError("renewal exploded")

        thread = HeartbeatThread(pub, lambda: {"v": 1}, interval_s=0.01, renewal_check=boom)
        thread.start()
        try:
            deadline = time.monotonic() + 2.0
            while ticks["n"] < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
            assert thread.is_alive()
            assert ticks["n"] >= 3
        finally:
            thread.stop()
        assert not thread.is_alive()  # stop() still joins cleanly afterward

    def test_renewal_check_runs_every_tick(self) -> None:
        pub = FakePublisher()
        calls = []
        thread = HeartbeatThread(
            pub, lambda: {"v": 1}, interval_s=0.01, renewal_check=lambda: calls.append(1)
        )
        thread.start()
        try:
            deadline = time.monotonic() + 2.0
            while len(calls) < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            thread.stop()
        assert len(calls) >= 3
