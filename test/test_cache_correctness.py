"""Regression tests for cache interpretation, publication, and concurrent queries."""

import pathlib
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import filelock
import pyarrow as pa
import pytest

import server
from settings import settings
from src import artifacts, cache, query
from src.source import base
from src.source.pyarrow.csv import SourceFactory

SAMPLE = "data/sample/pyarrow/csv/flight.csv"


def test_timestamp_options_cannot_reuse_default_timestamp_cache() -> None:
    server.query_messages(SAMPLE, 'SELECT COUNT(*) FROM "message"', "message")
    result = server.preview_pipeline(
        SAMPLE,
        "message",
        "message['accel_x'] < -10",
        5,
        5,
        2,
        {"timestamp_column": "t", "timestamp_format": "seconds"},
    )
    assert result["events"] == [20.0, 45.0]


def test_changes_beyond_hash_chunk_invalidate_source(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(base, "MD5_READ_SIZE", 8)
    path = tmp_path / "data.csv"
    path.write_text("t,value\n0,123456789\n")
    factory = SourceFactory(str(path))
    original = factory.uuid
    path.write_text("t,value\n0,123456780\n")
    assert factory.uuid != original
    modified = factory.uuid
    with path.open("a") as stream:
        stream.write("1,99\n")
    assert factory.uuid != modified


def _batches(value: int) -> Iterator[pa.RecordBatch]:
    yield pa.record_batch({"value": [value]})


def test_snapshot_survives_eviction_and_can_be_read_twice(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "snapshot.arrow"
    schema = next(_batches(1)).schema
    relation = cache.arrow_relation(path, schema, lambda: _batches(1), True)
    path.unlink()
    assert relation.fetchall() == [(1,)]
    assert relation.project("value + 1").fetchall() == [(2,)]


def test_failed_or_uncached_write_preserves_existing_cache(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "snapshot.arrow"
    schema = next(_batches(1)).schema
    cache.arrow_relation(path, schema, lambda: _batches(1), True)
    assert cache.arrow_relation(path, schema, lambda: _batches(2), False).fetchall() == [(2,)]

    def broken() -> Iterator[pa.RecordBatch]:
        yield from _batches(3)
        raise ValueError("decode failed")

    with pytest.raises(ValueError, match="decode failed"):
        cache.arrow_relation(path, schema, broken, False)
    assert cache.arrow_relation(path, schema, broken, True).fetchall() == [(1,)]
    assert not list(tmp_path.glob("*.partial*"))


def test_corrupt_cache_is_rebuilt(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "snapshot.arrow"
    path.write_bytes(b"interrupted")
    result = cache.arrow_relation(path, next(_batches(1)).schema, lambda: _batches(1), True)
    assert result.fetchall() == [(1,)]


def test_concurrent_cache_misses_publish_one_complete_snapshot(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "snapshot.arrow"
    barrier = threading.Barrier(2)
    calls = []

    def batches() -> Iterator[pa.RecordBatch]:
        calls.append(True)
        yield from _batches(1)

    def read() -> list:
        barrier.wait(timeout=10)
        return cache.arrow_relation(path, next(_batches(1)).schema, batches, True).fetchall()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(read) for _ in range(2)]
        assert [future.result(timeout=20) for future in futures] == [[(1,)], [(1,)]]
    assert len(calls) == 1


def test_same_topic_queries_are_isolated_between_threads() -> None:
    barrier = threading.Barrier(2)

    def read(value: int) -> list:
        relation = query.from_arrow(pa.table({"value": [value]}))
        result = query.sql(relation, "shared_topic", "SELECT * FROM shared_topic")
        barrier.wait(timeout=10)
        return result.fetchall()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(read, value) for value in (1, 2)]
        assert [future.result(timeout=20) for future in futures] == [[(1,)], [(2,)]]


def test_eviction_skips_locked_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    path = pathlib.Path(settings.CACHE_DIRECTORY) / "data/source_id=test/a.arrow"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"123")
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 1)
    with filelock.FileLock(str(path) + ".lock"):
        assert artifacts.evict_arrow_cache() == 0
    assert artifacts.evict_arrow_cache() == 1


def test_concurrent_eviction_passes_are_serialized(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two racing eviction passes must not each independently over-evict.

    Without a cache-wide eviction lock, two callers can each inventory the same
    over-limit cache using their own local running total, skip files the other
    is mid-deleting, and each delete different entries -- evicting far more
    than the single pass required. Serializing on one lock file makes a second,
    concurrent pass back off (return 0) instead of computing against stale
    sizes.
    """
    data_directory = pathlib.Path(settings.CACHE_DIRECTORY) / "data"
    path = data_directory / "source_id=test/a.arrow"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"123")
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 1)
    # Simulate another eviction pass already in flight by holding the
    # cache-wide eviction lock ourselves.
    with filelock.FileLock(str(data_directory / ".eviction.lock")):
        assert artifacts.evict_arrow_cache() == 0
    assert path.exists()
    assert artifacts.evict_arrow_cache() == 1


def test_cache_hit_is_served_even_when_entry_exceeds_cache_max_bytes(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single oversized entry must not evict-then-rebuild on every lookup."""
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 1)
    path = tmp_path / "data" / "source_id=abc" / "topics_x.arrow"
    schema = next(_batches(1)).schema
    calls = []

    def batches() -> Iterator[pa.RecordBatch]:
        calls.append(True)
        yield from _batches(1)

    cache.arrow_relation(path, schema, batches, True)
    assert len(calls) == 1

    result = cache.arrow_relation(path, schema, batches, True)
    assert result.fetchall() == [(1,)]
    assert len(calls) == 1  # served from cache, not rebuilt


def test_cache_hit_does_not_require_writable_lock_file(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read-only cache mount must still serve a cache hit for an existing entry."""
    path = tmp_path / "snapshot.arrow"
    schema = next(_batches(1)).schema
    cache.arrow_relation(path, schema, lambda: _batches(1), True)

    def deny_acquire(self: filelock.FileLock, *_a: object, **_k: object) -> None:
        raise OSError("Read-only file system")

    monkeypatch.setattr(filelock.FileLock, "acquire", deny_acquire)
    result = cache.arrow_relation(path, schema, lambda: _batches(999), True)
    assert result.fetchall() == [(1,)]
