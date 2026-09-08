"""Tests for the InfluxDB 3 data source.

Pure tests run everywhere. The end-to-end tests require a live InfluxDB 3 and are
gated on `BAGEL_INFLUXDB_TEST_URL`, e.g.:

    docker run -d --name bagel-influx -p 8181:8181 influxdb:3-core \
        influxdb3 serve --node-id node0 --object-store memory --without-auth
    BAGEL_INFLUXDB_TEST_URL=influxdb://localhost:8181/telemetry \
        uv run pytest test/source/test_influxdb.py
"""

import os
from datetime import datetime

import pytest

pytest.importorskip("influxdb_client_3")

import pyarrow as pa

from src.di.types import data_source
from src.message import influxdb as message_influxdb
from src.source import influxdb
from src.source.context import SourceContext
from src.topic.influxdb import TopicRegistry

INFLUX_URL = os.environ.get("BAGEL_INFLUXDB_TEST_URL")

requires_db = pytest.mark.skipif(
    not INFLUX_URL, reason="set BAGEL_INFLUXDB_TEST_URL to run against a live InfluxDB 3"
)


# -- pure ---------------------------------------------------------------------------


def test_influxdb_urls_resolve() -> None:
    assert data_source.resolve("influxdb://tok@h:8181/telemetry") == data_source.DataSource.INFLUXDB


def test_parse_url_with_token_and_port() -> None:
    parts = influxdb.parse_url("influxdb://s3cr3t@influx.local:8282/fleet")
    assert parts == {"host": "http://influx.local:8282", "database": "fleet", "token": "s3cr3t"}


def test_parse_url_defaults_port_and_allows_no_token() -> None:
    parts = influxdb.parse_url("influxdb://influx.local/fleet")
    assert parts["host"] == "http://influx.local:8181"
    assert parts["token"] == ""


def test_parse_url_unquotes_token() -> None:
    parts = influxdb.parse_url("influxdb://a%2Fb@h/db")
    assert parts["token"] == "a/b"  # noqa: S105 -- not a real credential


def test_parse_url_requires_database() -> None:
    with pytest.raises(ValueError, match="database"):
        influxdb.parse_url("influxdb://h:8181")


def test_parse_url_requires_influxdb_scheme() -> None:
    with pytest.raises(ValueError, match="influxdb://"):
        influxdb.parse_url("postgres://h/db")


def test_bounds_uses_aggregate_queries_not_full_row_downloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """bounds() must not download every row of every measurement.

    Regression for PR #237 review (src/source/context.py:50): `SourceContext.bounds()`
    called `to_duckdb()` with no `topics`, which downloads and struct-packs every row
    of every measurement (`_topic_arrow`'s `SELECT * FROM ... ORDER BY time`) merely
    to compute two numbers.
    """
    queries: list[str] = []

    class _FakeInfluxClient:
        def query(self, sql: str) -> pa.Table:
            queries.append(sql)
            if "information_schema.tables" in sql:
                return pa.table({"table_name": ["readings"]})
            return pa.table(
                {
                    "lo": pa.array([datetime(2024, 1, 1, 0, 0, 0)], type=pa.timestamp("us")),
                    "hi": pa.array([datetime(2024, 1, 1, 0, 2, 0)], type=pa.timestamp("us")),
                }
            )

    monkeypatch.setattr(
        influxdb.InfluxDatabase, "client", property(lambda self: _FakeInfluxClient())
    )
    data_source = influxdb.InfluxDatabase(host="http://h:8181", database="telemetry", token="")

    class _StubFactory:
        def build(self) -> influxdb.InfluxDatabase:
            return data_source

    context = SourceContext(
        factory=_StubFactory(),  # not a BoundedSourceFactory, so bounds() must query
        registry=TopicRegistry(),
        dataset=message_influxdb.MessageDataset(),
    )

    start, end = context.bounds()

    assert start == pytest.approx(1704067200.0)  # 2024-01-01T00:00:00Z
    assert end == pytest.approx(1704067320.0)  # 2024-01-01T00:02:00Z
    assert not any("SELECT *" in query for query in queries)


# -- live database ------------------------------------------------------------------


@requires_db
def test_end_to_end_over_live_influxdb() -> None:
    import server
    from src.di import module

    factory = module.provide("src.source.influxdb", {"path": INFLUX_URL})
    registry = module.provide("src.topic.influxdb", {})
    database = factory.build()

    topics = registry.available_topics(database)
    assert "readings" in topics
    assert registry.message_count("readings", database) > 0
    struct = registry.struct("readings", database)
    assert "temp" in struct.names
    assert "time" in struct.names
    assert "readings" in registry.describe("readings", database)

    rows = server.query_messages(
        path=INFLUX_URL,
        sql_statement=(
            'SELECT COUNT(*) AS n, MAX("readings"[\'temp\']) AS max_temp FROM "readings"'
        ),
        topic="readings",
    )
    assert rows[0]["n"] > 0
    assert rows[0]["max_temp"] is not None


@requires_db
def test_preview_pipeline_detects_events_in_influxdb() -> None:
    import server

    result = server.preview_pipeline(
        path=INFLUX_URL,
        event_topic="readings",
        predicate="\"readings\"['temp'] > 30",
        pre_seconds=60.0,
        post_seconds=60.0,
        debounce_seconds=120.0,
    )
    assert result["event_count"] == 2  # seeded by the harness
    assert 0 < result["kept_fraction"] < 1
