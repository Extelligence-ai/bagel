"""Secrets-leakage audit for issue #134.

Credentials (DSN passwords, InfluxDB tokens, MQTT broker credentials, Slack
webhook URLs) must never appear in an exception message, a `metadata` dict, or
a log line that could reach an MCP client. These tests drive each hardened
leak point with a secret-bearing input through its VALIDATION/parse-error path
(never a live server) and assert the secret substring is absent from whatever
came back, while useful diagnostics (host, database name) are preserved.
"""

import traceback

import pytest

from src.di.types import data_source
from src.pipeline.tasks.notify.slack import NotifySlack
from src.source import postgres
from src.source.redact import redact_url, scrub_secrets

# -- redact_url: the helper itself ---------------------------------------------------


def test_redact_url_strips_userinfo_but_keeps_host_and_db() -> None:
    safe = redact_url("postgresql://alice:s3cr3t@db.example.com:5432/prod")
    assert "s3cr3t" not in safe
    assert "alice" not in safe
    assert "db.example.com" in safe
    assert "prod" in safe


def test_redact_url_strips_sensitive_query_params() -> None:
    safe = redact_url("influxdb://influx.local:8181/fleet?token=ABC123")
    assert "ABC123" not in safe
    assert "influx.local" in safe
    assert "fleet" in safe


def test_redact_url_preserves_non_sensitive_query_params() -> None:
    safe = redact_url("https://example.com/path?region=us-east-1&token=ABC123")
    assert "region=us-east-1" in safe
    assert "ABC123" not in safe


def test_redact_url_passes_through_plain_path_unchanged() -> None:
    path = "./data/sample/pyarrow/csv/flight.csv"
    assert redact_url(path) == path
    assert redact_url("/data/logs/2026") == "/data/logs/2026"


def test_redact_url_handles_malformed_dsn_missing_slashes() -> None:
    # A typo'd DSN (missing "//") parses with the userinfo landing in `.path`
    # instead of `.netloc` -- the redaction must still catch it.
    safe = redact_url("postgres:alice:s3cr3t@host/db")
    assert "s3cr3t" not in safe
    assert "host" in safe


def test_redact_url_empty_string() -> None:
    assert redact_url("") == ""


def test_redact_url_fails_safe_on_malformed_ipv6_host() -> None:
    # Unbalanced `[` in the host makes `urlsplit` raise `ValueError` outright.
    # The old behavior handed the raw, unparsed `value` back on that path --
    # i.e. the password verbatim. Fail SAFE instead: never return the input.
    result = redact_url("postgres://alice:s3cr3t@[::1/db")
    assert "s3cr3t" not in result


def test_redact_url_fails_safe_on_unbalanced_bracket_in_host_only() -> None:
    result = redact_url("postgres://alice:s3cr3t@host[oops/db")
    assert "s3cr3t" not in result


def test_redact_url_fails_safe_on_bare_non_url_token() -> None:
    # Not a URL at all -- must not raise, and (trivially) has no secret to leak.
    assert redact_url("not-a-url-at-all") == "not-a-url-at-all"


def test_redact_url_malformed_dsn_with_bare_token_no_colon() -> None:
    # A missing-"//" InfluxDB URL typo: the userinfo is a BARE TOKEN with no
    # ":" in it. The malformed-DSN fallback branch used to only redact
    # `user:pass@host` forms (guarded on ":" in userinfo), so a bare token
    # sailed through unredacted. It must be redacted unconditionally on "@",
    # mirroring the well-formed netloc branch above.
    safe = redact_url("influxdb:MY-SECRET-TOKEN-abc123@influx.local:8181/fleet")
    assert "MY-SECRET-TOKEN-abc123" not in safe
    assert "influx.local" in safe


def test_redact_url_malformed_dsn_with_bare_token_no_colon_postgres() -> None:
    safe = redact_url("postgres:sekrittoken@host/db")
    assert "sekrittoken" not in safe
    assert "host" in safe


def test_redact_url_malformed_dsn_with_embedded_at_in_password() -> None:
    # On the malformed-DSN fallback path (missing "//"), a password containing
    # a literal "@" must be redacted using the LAST "@" before the path
    # boundary -- consistent with the well-formed netloc branch's
    # `rpartition("@")` -- not the first "@", which would leave the password's
    # tail ("evil") sitting unredacted in the output.
    safe = redact_url("postgres:alice:s3cr3t@evil@host/db")
    assert "s3cr3t" not in safe
    assert "evil" not in safe
    assert "host" in safe


def test_scrub_secrets_removes_verbatim_occurrences() -> None:
    text = "IO Error: could not connect using 'postgresql://alice:s3cr3t@host/db'"
    assert "s3cr3t" not in scrub_secrets(text, "s3cr3t")


def test_scrub_secrets_ignores_empty_or_none_secrets() -> None:
    text = "unaffected message"
    assert scrub_secrets(text, None, "") == text


# -- postgres: ConnectionError must not leak the DSN's password ---------------------


def test_postgres_connect_error_does_not_leak_password(monkeypatch: pytest.MonkeyPatch) -> None:
    import duckdb

    def _boom(_url: str) -> None:
        # Mirror DuckDB's real behavior: the postgres extension's own error message
        # embeds the raw connection string, password included.
        raise duckdb.IOException(
            "IO Error: Unable to connect to Postgres at "
            "postgresql://alice:s3cr3t@bad.invalid:5432/prod: name resolution failed"
        )

    monkeypatch.setattr(postgres, "attach", _boom)

    with pytest.raises(ConnectionError) as excinfo:
        postgres.SourceFactory("postgresql://alice:s3cr3t@bad.invalid:5432/prod")

    message = str(excinfo.value)
    assert "s3cr3t" not in message
    assert "bad.invalid" in message


def test_postgres_metadata_url_does_not_leak_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(postgres, "attach", lambda url: "pg_test")

    factory = postgres.SourceFactory("postgresql://alice:s3cr3t@db.example.com:5432/prod")
    monkeypatch.setattr(postgres.PostgresDatabase, "tables", lambda self: [])

    metadata = factory.metadata

    assert "s3cr3t" not in str(metadata)
    assert "db.example.com" in metadata["url"]


def test_postgres_metadata_does_not_leak_password_for_malformed_ipv6_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # End-to-end: a DSN with an unbalanced IPv6 bracket makes `urlsplit` raise
    # `ValueError` inside `redact_url`. That must fail SAFE, not hand the raw
    # (credential-bearing) DSN back into tool-facing `metadata`.
    monkeypatch.setattr(postgres, "attach", lambda url: "pg_test")
    monkeypatch.setattr(postgres.PostgresDatabase, "tables", lambda self: [])

    factory = postgres.SourceFactory("postgres://alice:s3cr3t@[::1/db")

    assert "s3cr3t" not in str(factory.metadata)


def test_postgres_connect_error_does_not_leak_password_for_malformed_ipv6_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A DSN with an unbalanced IPv6 bracket makes the *unguarded*
    # `urlsplit(path).password` call inside the `except duckdb.Error` handler raise
    # `ValueError` itself. Python chains the original `duckdb.Error` -- whose message
    # embeds the raw, credential-bearing connection string -- onto that new
    # `ValueError` as `__context__`, so the password leaks via the traceback even
    # though the handler never returns normally. Guard the extraction so this can't
    # happen, and prove it by inspecting the FULL chained traceback, not just the
    # raised exception's own message.
    #
    # The secret is built from a variable (never spelled out as a literal on the
    # same line as a `raise`/call statement) so that Python's own traceback
    # formatter -- which echoes each frame's *source line* verbatim from disk,
    # regardless of redaction -- can't trivially reintroduce it as a test
    # artifact unrelated to what postgres.py actually leaks.
    import duckdb

    secret_password = "s3cr3t"  # noqa: S105
    dsn = f"postgres://alice:{secret_password}@[::1/db"
    driver_message = f"IO Error: Unable to connect to Postgres at {dsn}: name resolution failed"

    def _boom(_url: str) -> None:
        raise duckdb.IOException(driver_message)

    monkeypatch.setattr(postgres, "attach", _boom)

    with pytest.raises(Exception) as excinfo:  # ValueError pre-fix, ConnectionError post-fix
        postgres.SourceFactory(dsn)

    full = "".join(traceback.format_exception(excinfo.value))
    assert secret_password not in full


# -- influxdb: parse/connection errors must not leak the token ----------------------


def test_influxdb_bad_scheme_error_does_not_leak_token() -> None:
    pytest.importorskip("influxdb_client_3")
    from src.source import influxdb

    with pytest.raises(ValueError) as excinfo:
        influxdb.parse_url("mysql://s3cr3ttoken@influx.local:8181/fleet")

    assert "s3cr3ttoken" not in str(excinfo.value)


def test_influxdb_missing_database_error_does_not_leak_token() -> None:
    pytest.importorskip("influxdb_client_3")
    from src.source import influxdb

    with pytest.raises(ValueError) as excinfo:
        influxdb.parse_url("influxdb://s3cr3ttoken@influx.local:8181")

    message = str(excinfo.value)
    assert "s3cr3ttoken" not in message
    assert "influx.local" in message


def test_influxdb_connect_error_does_not_leak_token(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("influxdb_client_3")
    from src.source import influxdb

    def _boom(self: object) -> None:
        raise RuntimeError("connection refused")

    monkeypatch.setattr(influxdb.InfluxDatabase, "tables", _boom)

    with pytest.raises(ConnectionError) as excinfo:
        influxdb.SourceFactory("influxdb://s3cr3ttoken@influx.local:8181/fleet")

    message = str(excinfo.value)
    assert "s3cr3ttoken" not in message
    assert "influx.local" in message


# -- slack: the webhook URL is itself the secret -- never echo it -------------------


def test_slack_rejects_non_http_webhook_without_echoing_it() -> None:
    secret_looking_value = "ftp://hooks.example.com/services/T00/B00/s3cr3twebhooktoken"  # noqa: S105

    with pytest.raises(ValueError, match="http") as excinfo:
        NotifySlack(webhook_url=secret_looking_value, message="x")

    assert "s3cr3twebhooktoken" not in str(excinfo.value)
    assert secret_looking_value not in str(excinfo.value)


# -- mqtt: broker credentials never appear in logs -----------------------------------
#
# `src/sink/mqtt.py` was audited and found NOT to leak: every `logging.*` call there
# is parameterized with a topic name, count, or duration -- never `username`/
# `password` -- and `TopicSink.metadata` (inherited from `src/sink/base.py`) only
# includes `host`/`port`/`available_topics`/`magic`. A full end-to-end regression
# test (constructing a real sink with credentials via the fake-paho-client fixture,
# then asserting the secrets are absent from both logs and metadata) lives in
# `test/sink/test_mqtt.py::test_broker_credentials_never_appear_in_logs_or_metadata`,
# since it needs that suite's `make_sink` fixture (not available to this directory).


# -- data_source.resolve: a malformed/typo'd DSN must not leak its credentials ------


def test_resolve_malformed_dsn_does_not_leak_credentials() -> None:
    # Missing "//" after the scheme: falls through to the file-based resolver,
    # which used to raise with the raw (credential-bearing) string verbatim.
    with pytest.raises(ValueError) as excinfo:
        data_source.resolve("postgres:alice:s3cr3t@host/db")

    message = str(excinfo.value)
    assert "s3cr3t" not in message
    assert "host" in message


def test_resolve_malformed_dsn_with_bare_token_does_not_leak_credentials() -> None:
    # Same missing-"//" fallthrough, but with a BARE TOKEN (no ":") as
    # userinfo -- a realistic InfluxDB URL typo. This is the end-to-end path
    # for the bare-token redact_url fix above: resolve() raises ValueError
    # (the string doesn't look like any supported file type), and that
    # ValueError's message must not contain the raw token.
    with pytest.raises(ValueError) as excinfo:
        data_source.resolve("influxdb:MY-SECRET-TOKEN-abc123@influx.local:8181/fleet")

    assert "MY-SECRET-TOKEN-abc123" not in str(excinfo.value)
