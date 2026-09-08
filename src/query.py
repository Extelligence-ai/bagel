"""Thread-owned DuckDB connections for queries and live subscription callbacks."""

import threading
from typing import cast

import duckdb

_local = threading.local()


def connection() -> duckdb.DuckDBPyConnection:
    """Return this thread's connection, retained for the lifetime of its relations.

    Relations must be consumed on their owning thread; return materialized results
    when crossing a thread boundary. Thread teardown releases the connection.
    """
    if not hasattr(_local, "connection"):
        _local.connection = duckdb.connect()
    return cast(duckdb.DuckDBPyConnection, _local.connection)


def from_arrow(table: object) -> duckdb.DuckDBPyRelation:
    """Create a relation on the calling thread's owned connection."""
    return connection().from_arrow(table)


def sql(relation: duckdb.DuckDBPyRelation, topic: str, statement: str) -> duckdb.DuckDBPyRelation:
    """Query a relation without registering names on DuckDB's global connection."""
    return relation.query(topic, statement)
