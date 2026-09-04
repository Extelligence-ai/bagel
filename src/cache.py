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


def arrow_relation(
    path: pathlib.Path,
    schema: pa.Schema,
    batches: Callable[[], Iterator[pa.RecordBatch]],
    use_cache: bool,
) -> duckdb.DuckDBPyRelation:
    """Read a complete snapshot or publish one under a per-entry interprocess lock.

    Failed writes never replace a valid entry. Uncached reads use a private file
    and do not invalidate another reader's cached result.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    artifacts.evict_arrow_cache()
    with filelock.FileLock(str(path) + ".lock"):
        if use_cache and path.exists():
            try:
                result = _snapshot(path)
            except (pa.ArrowInvalid, OSError):
                pass  # Interrupted writes from older versions are rebuilt.
            else:
                try:
                    os.utime(path)
                except OSError:
                    pass
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
