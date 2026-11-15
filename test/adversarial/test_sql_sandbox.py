"""GHSA-gx2g-3pch-43q5: model-supplied SQL can't read or write files, reach the network
or load extensions, and rsync options can't run commands.

Each attack is the reporter's proof of concept, sent through the real tool.
"""

import duckdb
import pyarrow as pa
import pytest

from bagel_mcp import query, server
from bagel_mcp.pipeline import live
from bagel_mcp.pipeline.tasks.rsync_files import RsyncFilesToLocalDirectory

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"
ARGS = {"timestamp_column": "t", "timestamp_format": "seconds"}


def test_select_still_works() -> None:
    rows = server.query_messages(
        SAMPLE, "SELECT min(message['accel_x']) AS lo FROM message", "message", args=ARGS
    )
    assert rows[0]["lo"] < -10


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM read_csv_auto('/etc/passwd')",
        "SELECT * FROM read_text('/etc/hosts')",
        "SELECT * FROM glob('/*')",
        "SELECT * FROM 'https://example.com/x.csv'",
        "SELECT (SELECT count(*) FROM read_text('/etc/hosts')) FROM message",
    ],
)
def test_select_cannot_reach_files_or_network(statement: str) -> None:
    with pytest.raises(duckdb.PermissionException):
        server.query_messages(SAMPLE, statement, "message", args=ARGS)


@pytest.mark.parametrize(
    "statement",
    [
        "COPY (SELECT 1) TO '/tmp/bagel-ghsa.csv'",
        "INSTALL httpfs",
        "LOAD httpfs",
        "ATTACH '/tmp/bagel-ghsa.db' AS e",
        "SET enable_external_access = true",
        "SELECT 1; SELECT 2",
    ],
)
def test_only_one_select_is_accepted(statement: str) -> None:
    with pytest.raises(query.UnsafeSqlError):
        server.query_messages(SAMPLE, statement, "message", args=ARGS)


def test_sandbox_settings_are_locked() -> None:
    with pytest.raises(duckdb.Error):
        query.sandbox().execute("SET enable_external_access = true")


def test_event_predicate_runs_in_the_sandbox() -> None:
    with pytest.raises(duckdb.PermissionException):
        server.preview_pipeline(
            path=SAMPLE,
            event_topic="message",
            predicate="(SELECT count(*) FROM read_text('/etc/hosts')) > 0",
            pre_seconds=1.0,
            args=ARGS,
        )


def test_live_predicate_runs_in_the_sandbox() -> None:
    struct = pa.struct([("x", pa.float64())])
    with pytest.raises(duckdb.PermissionException):
        live.evaluate_predicate(
            "t", struct, {"x": 1.0}, "(SELECT count(*) FROM read_text('/etc/hosts')) > 0"
        )


@pytest.mark.parametrize(
    "options",
    [
        ["-e", "sh -c 'touch /tmp/pwned'"],
        ["--rsh=sh -c id"],
        ["--rsync-path=sh -c id"],
        ["--log-file=/tmp/x"],
        ["-ave"],
        ["--quiet"],
        ["--log-format=%n"],
    ],
)
def test_rsync_rejects_options_off_the_allowlist(options: list[str]) -> None:
    with pytest.raises(ValueError, match="not allowed"):
        RsyncFilesToLocalDirectory("host:/data/", "out", options=options)


def test_rsync_rejects_a_source_that_looks_like_an_option() -> None:
    with pytest.raises(ValueError, match="must not start"):
        RsyncFilesToLocalDirectory("-e sh", "out")


def test_rsync_accepts_ordinary_options() -> None:
    RsyncFilesToLocalDirectory(
        "host:/data/", "out", options=["-avz", "--exclude=*.tmp", "--timeout=30"]
    )
