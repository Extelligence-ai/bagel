"""Thread-owned DuckDB connections for queries and live subscription callbacks.

SQL text that comes from a model or a pipeline config (query_messages statements, sql
gates, topic_sql tasks, event predicates) runs on a separate sandboxed connection that
can't touch files, the network or extensions (GHSA-gx2g-3pch-43q5). Data reaches it as
an Arrow stream from the thread's ordinary connection, which keeps the access that
sources such as Postgres need.
"""

import threading
from typing import cast

import duckdb

_local = threading.local()

# lock_configuration is applied after connect, so a statement can't switch these back.
_SANDBOX_CONFIG = {
    "enable_external_access": False,
    "autoinstall_known_extensions": False,
    "autoload_known_extensions": False,
    "python_enable_replacements": False,
}


class UnsafeSqlError(ValueError):
    """The SQL text is not a single read-only SELECT."""


def connection() -> duckdb.DuckDBPyConnection:
    """Return this thread's connection, retained for the lifetime of its relations.

    Relations must be consumed on their owning thread; return materialized results
    when crossing a thread boundary. Thread teardown releases the connection.
    """
    if not hasattr(_local, "connection"):
        _local.connection = duckdb.connect()
    return cast(duckdb.DuckDBPyConnection, _local.connection)


def sandbox() -> duckdb.DuckDBPyConnection:
    """Return this thread's sandboxed connection: no files, network or extensions."""
    if not hasattr(_local, "sandbox"):
        sandboxed = duckdb.connect(config=_SANDBOX_CONFIG)
        sandboxed.execute("SET lock_configuration = true")
        _local.sandbox = sandboxed
    return cast(duckdb.DuckDBPyConnection, _local.sandbox)


def from_arrow(table: object) -> duckdb.DuckDBPyRelation:
    """Create a relation on the calling thread's owned connection."""
    return connection().from_arrow(table)


def untrusted(relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Stream a relation into the sandbox, where untrusted SQL text may be applied to it."""
    return sandbox().from_arrow(relation.fetch_arrow_reader())


def check_select(statement: str) -> None:
    """Raise UnsafeSqlError unless the statement is exactly one SELECT."""
    try:
        statements = duckdb.extract_statements(statement)
    except duckdb.Error as error:
        raise UnsafeSqlError(f"SQL does not parse: {error}") from error
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise UnsafeSqlError(
            "Only a single read-only SELECT statement is allowed (no COPY, INSTALL, "
            "ATTACH, SET or multiple statements)"
        )


def sql(relation: duckdb.DuckDBPyRelation, topic: str, statement: str) -> duckdb.DuckDBPyRelation:
    """Run one untrusted SELECT over a relation, in the sandbox.

    The relation is registered under the topic name only inside the sandbox, never on
    DuckDB's global connection.
    """
    check_select(statement)
    return untrusted(relation).query(topic, statement)


def predicate_hits(
    relation: duckdb.DuckDBPyRelation, ts_column: str, predicate: str
) -> duckdb.DuckDBPyRelation:
    """Evaluate an untrusted boolean predicate per row, in the sandbox, as (ts, hit)."""
    return untrusted(relation).project(f"{ts_column} AS ts, ({predicate}) AS hit").order("ts")
