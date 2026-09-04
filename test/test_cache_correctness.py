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
