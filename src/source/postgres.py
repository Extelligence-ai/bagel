"""A data source factory for PostgreSQL / TimescaleDB databases.

Connects through DuckDB's `postgres` extension (read-only ATTACH), so no separate
database driver is needed. Each table is exposed as a Bagel "topic"; a timestamp
column is detected per table (or overridden) and converted to epoch seconds.

The data source `path` is a standard connection URL, e.g.
``postgres://user:pass@host:5432/dbname``.
"""

import functools
import re
import uuid
from typing import Any
from urllib.parse import urlsplit

import duckdb
from pydantic import BaseModel, Field

from src.di import module
from src.source.redact import REDACTED, redact_url, scrub_secrets

# Matches a `user:password@` fragment embedded anywhere in arbitrary text (e.g. an
# underlying driver's own error message that re-embeds a raw connection string).
# Used as a defense-in-depth fallback that doesn't require knowing the password
# value ahead of time -- unlike `scrub_secrets`, it works even when the DSN is too
# malformed for `urlsplit` to parse it out.
_EMBEDDED_USERINFO_RE = re.compile(r"(://[^:@/\s]+:)([^@/\s]+)(@)")


def _scrub_embedded_userinfo(text: str) -> str:
    """Redact any `scheme://user:password@` fragment found within `text`."""
    return _EMBEDDED_USERINFO_RE.sub(rf"\1{REDACTED}\3", text)


EXCLUDED_SCHEMAS = (
    "pg_catalog",
    "information_schema",
    "_timescaledb_catalog",
    "_timescaledb_internal",
    "_timescaledb_config",
    "_timescaledb_cache",
    "timescaledb_information",
    "timescaledb_experimental",
)

# Preferred timestamp column names, in order (case-insensitive).
TIMESTAMP_NAME_PREFERENCE = ("time", "timestamp", "ts", "created_at", "recorded_at", "datetime")


def quote_identifier(name: str) -> str:
    """Quote an SQL identifier."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def attach_name(url: str) -> str:
    """Return a deterministic DuckDB catalog name for the attached database."""
    return "pg_" + uuid.uuid5(uuid.NAMESPACE_URL, url).hex[:12]


@functools.lru_cache
def attach(url: str) -> str:
    """Attach the database read-only on the global DuckDB connection (once per URL).

    Returns:
        The DuckDB catalog name of the attached database.

    """
    duckdb.install_extension("postgres")
    duckdb.load_extension("postgres")
    name = attach_name(url)
    safe_url = url.replace("'", "''")
    duckdb.execute(f"ATTACH IF NOT EXISTS '{safe_url}' AS {name} (TYPE postgres, READ_ONLY)")
    return name


class PostgresDatabase(BaseModel):
    """Represent a PostgreSQL/TimescaleDB data source."""

    url: str
    # topic -> timestamp column override
    timestamp_columns: dict[str, str] = Field(default_factory=dict)

    @property
    def catalog(self) -> str:
        """The DuckDB catalog name of the attached database."""
        return attach(self.url)

    def tables(self) -> list[tuple[str, str]]:
        """Return (schema, table) pairs of the user tables in the database."""
        excluded = ", ".join(f"'{schema}'" for schema in EXCLUDED_SCHEMAS)
        rows = duckdb.execute(
            f"SELECT schema_name, table_name FROM duckdb_tables() "  # noqa: S608
            f"WHERE database_name = '{self.catalog}' AND schema_name NOT IN ({excluded}) "
            f"ORDER BY schema_name, table_name"
        ).fetchall()
        return [(schema, table) for schema, table in rows]

    def topic_of(self, schema: str, table: str) -> str:
        """Return the topic name for a table (bare name for the public schema)."""
        return table if schema == "public" else f"{schema}.{table}"

    def relation_name(self, topic: str) -> str:
        """Return the fully qualified, quoted DuckDB relation name for a topic."""
        schema, _, table = topic.rpartition(".")
        schema = schema or "public"
        return f"{self.catalog}.{quote_identifier(schema)}.{quote_identifier(table)}"

    def columns(self, topic: str) -> list[tuple[str, str]]:
        """Return (column name, DuckDB type) pairs for a topic's table."""
        rows = duckdb.execute(f"DESCRIBE {self.relation_name(topic)}").fetchall()
        return [(row[0], row[1]) for row in rows]

    def timestamp_column(self, topic: str) -> str:
        """Return the timestamp column for a topic (override, else detected).

        Detection prefers well-known column names, then falls back to the first
        TIMESTAMP/DATE-typed column.

        Raises:
            ValueError: If no timestamp column can be determined.

        """
        if topic in self.timestamp_columns:
            return self.timestamp_columns[topic]

        columns = self.columns(topic)
        time_typed = [
            name for name, type_ in columns if "TIMESTAMP" in type_.upper() or type_ == "DATE"
        ]
        for preferred in TIMESTAMP_NAME_PREFERENCE:
            for name in time_typed:
                if name.lower() == preferred:
                    return name
        if time_typed:
            return time_typed[0]
        raise ValueError(
            f"No timestamp column found for '{topic}'. "
            f"Pass one explicitly via args: {{'timestamp_columns': {{'{topic}': '<column>'}}}}"
        )

    def __hash__(self) -> int:
        """Needed for functools caching."""
        return hash(self.url)


class SourceFactory:
    """A data source factory for PostgreSQL/TimescaleDB databases."""

    def __init__(self, path: str, timestamp_columns: dict[str, str] | None = None) -> None:
        """Initialize the PostgreSQL data source factory.

        Args:
            path (str): The connection URL, e.g. ``postgres://user:pass@host:5432/db``.
            timestamp_columns (dict[str, str] | None, optional): Per-topic overrides for
                the timestamp column, e.g. ``{"readings": "recorded_at"}``. If omitted,
                the column is detected (preferring names like time/timestamp/created_at).

        Raises:
            ConnectionError: If the database cannot be attached.

        """
        self._url = path
        self._timestamp_columns = timestamp_columns or {}
        try:
            attach(path)
        except duckdb.Error as error:
            # DuckDB's postgres extension embeds the *raw* connection string (password
            # included) in its own error message, so redacting `path` alone is not
            # enough -- scrub the actual password out of the combined message too.
            # `urlsplit` can itself raise `ValueError` on a malformed host (e.g. an
            # unbalanced IPv6 bracket); if it did we'd be inside this `except` block,
            # so Python would chain the original `error` -- password and all -- onto
            # that new ValueError as `__context__`, leaking the password via the
            # traceback even though this handler never raises `ConnectionError`
            # normally. Guard the extraction so that can't happen: no password just
            # means `scrub_secrets` below is a harmless no-op (the regex fallback
            # below still catches it).
            try:
                password = urlsplit(path).password
            except ValueError:
                password = None
            message = _scrub_embedded_userinfo(
                scrub_secrets(
                    f"Cannot connect to '{redact_url(path)}': {error}",
                    password,
                )
            )
            # `raise ... from error` chains the *original*, unmodified `error` into
            # the traceback too, so scrub its own message in place -- otherwise the
            # password survives in the "above exception" section of a full
            # traceback dump regardless of how well `message` above is redacted.
            if error.args:
                error.args = tuple(
                    _scrub_embedded_userinfo(scrub_secrets(arg, password))
                    if isinstance(arg, str)
                    else arg
                    for arg in error.args
                )
            raise ConnectionError(message) from error

    @property
    def path(self) -> str:
        """The connection URL."""
        return self._url

    @property
    def uuid(self) -> str:
        """A unique identifier for the data source."""
        return str(uuid.uuid5(uuid.NAMESPACE_URL, self._url))

    def build(self) -> PostgresDatabase:
        """Return a PostgresDatabase object."""
        return PostgresDatabase(url=self._url, timestamp_columns=self._timestamp_columns)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the database: tables, row counts, and columns."""
        database = self.build()
        tables = []
        for schema, table in database.tables():
            topic = database.topic_of(schema, table)
            (count,) = duckdb.execute(
                f"SELECT COUNT(*) FROM {database.relation_name(topic)}"  # noqa: S608
            ).fetchone()
            tables.append(
                {
                    "topic": topic,
                    "row_count": count,
                    "columns": [
                        {"name": name, "type": type_} for name, type_ in database.columns(topic)
                    ],
                }
            )
        return {"url": redact_url(self._url), "tables": tables}


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
