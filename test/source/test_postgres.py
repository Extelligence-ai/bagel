"""Tests for the PostgreSQL/TimescaleDB data source.

Pure tests run everywhere. The end-to-end tests require a live database and are
gated on `BAGEL_POSTGRES_TEST_URL`, e.g.:

    docker run -d --name bagel-pg -p 5433:5432 -e POSTGRES_PASSWORD=bagel postgres:16-alpine
    BAGEL_POSTGRES_TEST_URL=postgres://postgres:bagel@localhost:5433/postgres \
        uv run pytest test/source/test_postgres.py
"""

import os

import pytest

from src.di.types import data_source
from src.message import postgres as message_postgres
from src.query import connection
from src.source import postgres
from src.source.context import SourceContext
from src.topic.postgres import TopicRegistry

PG_URL = os.environ.get("BAGEL_POSTGRES_TEST_URL")

requires_db = pytest.mark.skipif(
    not PG_URL, reason="set BAGEL_POSTGRES_TEST_URL to run against a live database"
)


# -- pure ---------------------------------------------------------------------------


def test_postgres_urls_resolve() -> None:
    assert data_source.resolve("postgres://u:p@h:5432/db") == data_source.DataSource.POSTGRES
    assert data_source.resolve("postgresql://u:p@h:5432/db") == data_source.DataSource.POSTGRES


def test_unknown_url_scheme_raises_with_supported_list() -> None:
    with pytest.raises(NotImplementedError, match="postgres"):
        data_source.resolve("mysql://u:p@h:3306/db")


def test_quote_identifier_escapes_quotes() -> None:
    assert postgres.quote_identifier("plain") == '"plain"'
    assert postgres.quote_identifier('we"ird') == '"we""ird"'


def test_attach_name_is_deterministic_and_distinct() -> None:
    a = postgres.attach_name("postgres://h/db1")
    assert a == postgres.attach_name("postgres://h/db1")
    assert a != postgres.attach_name("postgres://h/db2")
    assert a.isidentifier()


def test_timestamp_column_prefers_wellknown_names(monkeypatch: pytest.MonkeyPatch) -> None:
    database = postgres.PostgresDatabase(url="postgres://h/db")
    monkeypatch.setattr(
        postgres.PostgresDatabase,
        "columns",
        lambda self, topic: [
            ("id", "BIGINT"),
            ("updated", "TIMESTAMP WITH TIME ZONE"),
            ("time", "TIMESTAMP WITH TIME ZONE"),
        ],
    )
    assert database.timestamp_column("readings") == "time"


def test_timestamp_column_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    database = postgres.PostgresDatabase(
        url="postgres://h/db", timestamp_columns={"readings": "logged_at"}
    )
    assert database.timestamp_column("readings") == "logged_at"


def test_timestamp_column_missing_raises_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = postgres.PostgresDatabase(url="postgres://h/db")
    monkeypatch.setattr(
        postgres.PostgresDatabase, "columns", lambda self, topic: [("id", "BIGINT")]
    )
    with pytest.raises(ValueError, match="timestamp_columns"):
        database.timestamp_column("readings")


def test_bounds_skips_tables_without_a_detectable_timestamp_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A table lacking a timestamp column must not block bounds() on the others.

    Regression for PR #237 review (src/source/context.py:50): `SourceContext.bounds()`
    called `to_duckdb()` with no `topics`, so it expanded to every table via
    `_topic_select`, which calls `timestamp_column()` unconditionally -- raising
    for any unrelated table lacking a detectable timestamp column even though only
    the eligible tables matter for the source's overall bounds.
    """
    monkeypatch.setattr(postgres, "attach", lambda url: "pg_test")
    monkeypatch.setattr(
        postgres.PostgresDatabase, "relation_name", lambda self, topic: f'"{topic}"'
    )
    monkeypatch.setattr(
        postgres.PostgresDatabase,
        "tables",
        lambda self: [("public", "events"), ("public", "opaque")],
    )

    connection().execute("CREATE OR REPLACE TABLE events (t TIMESTAMP, v INTEGER)")
    connection().execute(
        "INSERT INTO events VALUES "
        "(TIMESTAMP '2024-01-01 00:00:00', 1), (TIMESTAMP '2024-01-01 00:02:00', 2)"
    )
    connection().execute("CREATE OR REPLACE TABLE opaque (v INTEGER)")
    connection().execute("INSERT INTO opaque VALUES (1), (2)")

    factory = postgres.SourceFactory(path="postgres://h/db")
    context = SourceContext(
        factory=factory, registry=TopicRegistry(), dataset=message_postgres.MessageDataset()
    )

    start, end = context.bounds()

    assert start == pytest.approx(1704067200.0)  # 2024-01-01T00:00:00Z
    assert end == pytest.approx(1704067320.0)  # 2024-01-01T00:02:00Z


# -- live database ------------------------------------------------------------------


@requires_db
def test_end_to_end_over_live_database() -> None:
    import server
    from src.di import module

    factory = module.provide("src.source.postgres", {"path": PG_URL})
    registry = module.provide("src.topic.postgres", {})
    database = factory.build()

    topics = registry.available_topics(database)
    assert "readings" in topics
    assert registry.message_count("readings", database) > 0
    struct = registry.struct("readings", database)
    assert "temp" in struct.names
    assert "readings" in registry.describe("readings", database)

    rows = server.query_messages(
        path=PG_URL,
        sql_statement=(
            'SELECT COUNT(*) AS n, MAX("readings"[\'temp\']) AS max_temp FROM "readings"'
        ),
        topic="readings",
    )
    assert rows[0]["n"] > 0
    assert rows[0]["max_temp"] is not None


@requires_db
def test_preview_pipeline_detects_events_in_database() -> None:
    import server

    result = server.preview_pipeline(
        path=PG_URL,
        event_topic="readings",
        predicate="\"readings\"['temp'] > 30",
        pre_seconds=60.0,
        post_seconds=60.0,
        debounce_seconds=120.0,
    )
    assert result["event_count"] == 2  # seeded by the harness below
    assert 0 < result["kept_fraction"] < 1


@requires_db
def test_bounds_ignore_tables_without_a_timestamp_column() -> None:
    """A lookup table with no timestamp column must not break whole-source bounds.

    Whole-source bounds enumerate every topic (table) unless told otherwise; an
    ordinary lookup table (`lookup_codes_no_timestamp`, seeded by the harness
    alongside `readings`) used to make `preview_pipeline`/reduce fail before ever
    evaluating the event topic, because bounds required every table -- including
    unrelated ones -- to have a resolvable timestamp column.
    """
    import server

    result = server.preview_pipeline(
        path=PG_URL,
        event_topic="readings",
        predicate="\"readings\"['temp'] > 30",
        pre_seconds=60.0,
        post_seconds=60.0,
        debounce_seconds=120.0,
    )
    assert result["event_count"] == 2
