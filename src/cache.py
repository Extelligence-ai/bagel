"""Atomic Arrow cache publication and memory-mapped snapshots safe across eviction."""

import os
import pathlib
import tempfile
from collections.abc import Callable, Iterator

import duckdb
import filelock
import pyarrow as pa

from src import artifacts, query


def _snapshot(path: pathlib.Path) -> duckdb.DuckDBPyRelation:
    # Arrow buffers retain their mapped file even after eviction unlinks the path.
    # read_all assembles zero-copy batches, not decoded Python records.
    with pa.memory_map(str(path), "r") as mapped:
        table = pa.ipc.open_file(mapped).read_all()
    return query.from_arrow(table)


def _cached_hit(path: pathlib.Path, use_cache: bool) -> duckdb.DuckDBPyRelation | None:
    """Return the existing entry's snapshot, or None if there isn't a usable one."""
    if not (use_cache and path.exists()):
        return None
    try:
        result = _snapshot(path)
    except (pa.ArrowInvalid, OSError):
        return None  # Interrupted writes from older versions are rebuilt.
    try:
        os.utime(path)
    except OSError:
        pass  # a read-only cache mount must not break reads
    return result


def arrow_relation(
    path: pathlib.Path,
    schema: pa.Schema,
    batches: Callable[[], Iterator[pa.RecordBatch]],
    use_cache: bool,
) -> duckdb.DuckDBPyRelation:
    """Read a complete snapshot or publish one under a per-entry interprocess lock.

    Failed writes never replace a valid entry. Uncached reads use a private file
    and do not invalidate another reader's cached result. A cache hit is read
    without acquiring the lock first: a read-only cache mount (no write access to
    the cache directory, so it cannot hold `.lock` files either) must still be
    able to serve existing entries, and concurrent readers should not serialize
    behind a lock they don't need.
    """
    if (result := _cached_hit(path, use_cache)) is not None:
        return result
    path.parent.mkdir(parents=True, exist_ok=True)
    artifacts.evict_arrow_cache()
    with filelock.FileLock(str(path) + ".lock"):
        if (result := _cached_hit(path, use_cache)) is not None:
            return result
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".partial") as temporary:
            with pa.OSFile(temporary.name, "wb") as sink, pa.ipc.new_file(sink, schema) as writer:
                for batch in batches():
                    writer.write_batch(batch)
            result = _snapshot(pathlib.Path(temporary.name))
            if use_cache:
                # Keep NamedTemporaryFile's own name for its cleanup after publication.
                staging = pathlib.Path(temporary.name + ".publish")
                try:
                    os.link(temporary.name, staging)
                    os.replace(staging, path)
                finally:
                    staging.unlink(missing_ok=True)
            return result
