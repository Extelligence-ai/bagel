"""Bounded artifact store + JSON-encoded MCAP event-window writer (fleet step 8)."""

import importlib
import json
import os
import pathlib
import sys

import pyarrow as pa
import pytest
from mcap.reader import make_reader

from settings import settings
from src.pipeline.lichtblick import jsonschema_type
from src.sink.publish.artifacts import ArtifactStore, write_event_mcap

SECOND_NS = 1_000_000_000

STRUCT = pa.struct(
    [
        pa.field("accel", pa.float64()),
        pa.field("meta", pa.struct([pa.field("flag", pa.bool_())])),
    ]
)


def _samples(n: int, start: float = 1_700_000_000.0) -> list[tuple[float, dict]]:
    return [
        (start + i * 0.1, {"accel": float(i) * 1.5, "meta": {"flag": i % 2 == 0}}) for i in range(n)
    ]


class TestWriteEventMcap:
    def test_round_trip(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "event.mcap"
        samples = _samples(5)

        count = write_event_mcap(path, "imu", STRUCT, samples)

        assert count == 5
        with open(path, "rb") as stream:
            messages = list(make_reader(stream).iter_messages())
        assert len(messages) == 5
        for (schema, channel, message), (t, payload) in zip(messages, samples, strict=True):
            assert channel.topic == "imu"
            assert schema.encoding == "jsonschema"
            assert json.loads(schema.data) == jsonschema_type(STRUCT)
            assert channel.message_encoding == "json"
            assert json.loads(message.data) == payload
            assert message.log_time == int(t * SECOND_NS)


class TestForRobot:
    def test_rejects_dot_dot_prefixed(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
        with pytest.raises(ValueError):
            ArtifactStore.for_robot("../x")

    def test_rejects_too_many_segments(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
        with pytest.raises(ValueError):
            ArtifactStore.for_robot("a/b/c")

    def test_rejects_empty(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
        with pytest.raises(ValueError):
            ArtifactStore.for_robot("")

    def test_rejects_leading_slash(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
        with pytest.raises(ValueError):
            ArtifactStore.for_robot("/x")

    def test_rejects_dot(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
        with pytest.raises(ValueError):
            ArtifactStore.for_robot(".")

    def test_two_segment_roots_correctly(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
        store = ArtifactStore.for_robot("t/r")
        assert store._root == tmp_path / "publish-artifacts" / "t" / "r"
        assert store._root.exists()


class TestStore:
    @pytest.fixture()
    def store(self, tmp_path: pathlib.Path) -> ArtifactStore:
        return ArtifactStore(tmp_path / "artifacts", max_bytes=10 * 1024 * 1024)

    def test_names_file_and_writes_no_siblings(
        self, store: ArtifactStore, tmp_path: pathlib.Path
    ) -> None:
        path = store.store("hard_decel", "evt-123", "imu", STRUCT, _samples(3))

        assert path == store._root / "hard_decel-evt-123.mcap"
        assert path.exists()
        siblings = list(store._root.iterdir())
        assert siblings == [path]

    def test_rejects_dot_dot_name_and_writes_nothing(self, store: ArtifactStore) -> None:
        with pytest.raises(ValueError):
            store.store("..", "evt-123", "imu", STRUCT, _samples(3))
        assert list(store._root.iterdir()) == []

    def test_oversized_write_returns_none_and_directory_unchanged(
        self, tmp_path: pathlib.Path
    ) -> None:
        tiny_store = ArtifactStore(tmp_path / "artifacts", max_bytes=10)

        result = tiny_store.store("hard_decel", "evt-1", "imu", STRUCT, _samples(5))

        assert result is None
        assert list(tiny_store._root.iterdir()) == []

    def test_rejects_slash_event_id_and_writes_nothing(self, store: ArtifactStore) -> None:
        with pytest.raises(ValueError):
            store.store("hard_decel", "a/b", "imu", STRUCT, _samples(3))
        assert list(store._root.iterdir()) == []

    def test_rejects_dot_dot_event_id_and_writes_nothing(self, store: ArtifactStore) -> None:
        with pytest.raises(ValueError):
            store.store("hard_decel", "..", "imu", STRUCT, _samples(3))
        assert list(store._root.iterdir()) == []


class TestEviction:
    def test_cap_sized_for_two_files_evicts_two_oldest_of_four(
        self, tmp_path: pathlib.Path
    ) -> None:
        probe = ArtifactStore(tmp_path / "probe", max_bytes=10 * 1024 * 1024)
        probe_path = probe.store("probe", "evt-0", "imu", STRUCT, _samples(3))
        one_file_bytes = probe_path.stat().st_size

        store = ArtifactStore(tmp_path / "artifacts", max_bytes=2 * one_file_bytes)
        written = []
        for i in range(4):
            path = store.store("hard_decel", f"evt-{i}", "imu", STRUCT, _samples(3))
            assert path is not None
            # Force a strictly increasing mtime regardless of filesystem
            # timestamp resolution, so "oldest" is deterministic.
            os.utime(path, (i, i))
            written.append(path)

        remaining = set(store._root.iterdir())
        assert remaining == {written[2], written[3]}
        assert store.stats()["bytes"] <= store._max_bytes
        assert store.stats()["files"] == 2

    def test_just_written_file_survives_eviction_even_when_it_ties_or_loses_on_mtime(
        self, tmp_path: pathlib.Path
    ) -> None:
        """The just-written file must be excluded from eviction by identity.

        Not by mtime: stamp the two pre-existing files with a mtime FAR in the
        future (tied with each other), so the just-written third file's real
        (present-day) mtime is the numerically OLDEST of the three. A plain
        "oldest by mtime among all files" eviction would wrongly pick the
        file that was just written; excluding it by identity must not.
        """
        probe = ArtifactStore(tmp_path / "probe", max_bytes=10 * 1024 * 1024)
        probe_path = probe.store("probe", "evt-0", "imu", STRUCT, _samples(3))
        one_file_bytes = probe_path.stat().st_size

        store = ArtifactStore(tmp_path / "artifacts", max_bytes=2 * one_file_bytes)
        far_future = 4_102_444_800.0  # year 2100
        p1 = store.store("hard_decel", "evt-1", "imu", STRUCT, _samples(3))
        os.utime(p1, (far_future, far_future))
        p2 = store.store("hard_decel", "evt-2", "imu", STRUCT, _samples(3))
        os.utime(p2, (far_future, far_future))  # tied with p1, both "newer" than p3 will be

        p3 = store.store("hard_decel", "evt-3", "imu", STRUCT, _samples(3))

        assert p3 is not None
        assert p3.exists()
        remaining = set(store._root.iterdir())
        assert p3 in remaining
        assert remaining <= {p1, p2, p3}
        assert store.stats()["bytes"] <= store._max_bytes


class TestStats:
    def test_empty_store(self, tmp_path: pathlib.Path) -> None:
        store = ArtifactStore(tmp_path / "artifacts", max_bytes=1024)
        assert store.stats() == {"bytes": 0, "files": 0}

    def test_reflects_written_files(self, tmp_path: pathlib.Path) -> None:
        store = ArtifactStore(tmp_path / "artifacts", max_bytes=10 * 1024 * 1024)
        store.store("hard_decel", "evt-1", "imu", STRUCT, _samples(3))
        stats = store.stats()
        assert stats["files"] == 1
        assert stats["bytes"] > 0

    def test_file_deleted_between_glob_and_stat_is_skipped(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review (P2, artifacts.py:228): `stats()` snapshots paths
        with `glob()` and then `stat()`s each one; a concurrent eviction (or
        the documented collector-sidecar workflow) can remove a file in
        between, raising `FileNotFoundError` and degrading status to an
        error. A vanished entry must be skipped, not fatal."""
        store = ArtifactStore(tmp_path / "artifacts", max_bytes=10 * 1024 * 1024)
        store.store("hard_decel", "evt-1", "imu", STRUCT, _samples(3))
        store.store("hard_decel", "evt-2", "imu", STRUCT, _samples(3))

        real_stat = pathlib.Path.stat

        def flaky_stat(self: pathlib.Path, *args: object, **kwargs: object) -> os.stat_result:
            if "evt-2" in self.name:
                raise FileNotFoundError(self)
            return real_stat(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "stat", flaky_stat)

        stats = store.stats()
        assert stats["files"] == 1
        assert stats["bytes"] > 0


def test_artifacts_module_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """artifacts.py must not drag paho or cryptography at import time."""
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.artifacts", raising=False)
    importlib.import_module("src.sink.publish.artifacts")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )
