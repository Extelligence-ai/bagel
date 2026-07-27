"""Redact credentials from URLs/DSNs before they reach errors, logs, or metadata.

Several data sources and sinks (PostgreSQL, InfluxDB, MQTT, Slack webhooks, ...)
are configured with a URL or connection string that embeds a credential -- a
password in the userinfo (``user:pass@host``), a token in a query parameter
(``?token=...``), or the entire URL being the secret (a webhook URL). Those
raw values must never be echoed back into an exception message, a log line, or
a tool-facing ``metadata`` dict, since all three can reach an MCP client.

This module provides two small, composable helpers:

- `redact_url`: builds a *display-safe* version of a URL for use in error
  messages and metadata, keeping the scheme/host/path (useful for diagnosing
  "which database/host did this fail against") while stripping userinfo and
  known-sensitive query parameters.
- `scrub_secrets`: a defense-in-depth net for text that isn't under our
  control, e.g. an underlying driver's own exception message, which can
  re-embed the raw secret even after we've redacted our own message.
"""

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED = "***"

# Query-parameter names (case-insensitive) whose values are redacted by `redact_url`.
SENSITIVE_QUERY_PARAMS = frozenset(
    {
        "token",
        "password",
        "passwd",
        "pwd",
        "apikey",
        "api_key",
        "key",
        "secret",
        "access_key",
        "secret_key",
        "sas",
        "sig",
        "signature",
    }
)


def redact_url(value: str) -> str:
    """Return a display-safe version of a URL/DSN with credentials removed.

    Userinfo is replaced wholesale (``user:pass@host`` -> ``***@host``), and
    any query parameter whose name matches `SENSITIVE_QUERY_PARAMS` (case
    insensitive) has its value replaced with `REDACTED`. The scheme, host,
    port, path, and any non-sensitive query parameters are preserved so the
    result stays useful for diagnostics (e.g. "which host/db was this?"). Any
    URL fragment is dropped unconditionally: no current call site uses
    fragments, and some APIs (e.g. OAuth implicit-grant redirects) stuff
    credentials there, so there's no safe way to tell a benign fragment from
    a sensitive one.

    A value with no userinfo, no sensitive query parameters, and no fragment
    -- including a plain filesystem path with no URL structure at all -- is
    returned unchanged.
    """
    try:
        parts = urlsplit(value)
    except ValueError:
        # Value isn't parseable as a URL at all (e.g. malformed IPv6 host).
        # We have no safe way to reconstruct a redacted version of it, and
        # `value` itself may well be (or contain) the raw credential -- so
        # fail SAFE and hand back a fixed placeholder, never the input.
        return REDACTED

    netloc = parts.netloc
    if "@" in netloc:
        _, _, host_part = netloc.rpartition("@")
        netloc = f"{REDACTED}@{host_part}" if host_part else REDACTED

    path = parts.path
    if parts.scheme and not parts.netloc:
        # A scheme with no netloc means `urlsplit` couldn't find a `//` authority --
        # likely a malformed DSN (missing slashes) whose userinfo landed in `.path`.
        # Split off the actual path/db (after the first `/`) before looking for
        # userinfo, then anchor on the LAST `@` in what's left -- mirroring the
        # netloc branch's `rpartition("@")` above -- so a password that itself
        # contains a literal `@` doesn't leave its tail unredacted.
        head, sep, tail = path.partition("/")
        userinfo, at_sign, host_part = head.rpartition("@")
        if at_sign:
            path = f"{REDACTED}@{host_part}{sep}{tail}"

    query = parts.query
    if query:
        pairs = parse_qsl(query, keep_blank_values=True)
        query = urlencode(
            [(k, REDACTED if k.lower() in SENSITIVE_QUERY_PARAMS else v) for k, v in pairs],
            safe="*",
        )

    # Fragments aren't used by any current call site, and unlike the path/query
    # we don't have a safe way to know whether a fragment carries a credential
    # (some APIs stuff tokens there, e.g. OAuth implicit-grant redirects) --
    # so drop it unconditionally rather than passing it through unredacted.
    fragment = ""

    return urlunsplit((parts.scheme, netloc, path, query, fragment))


def scrub_secrets(text: str, *secrets: str | None) -> str:
    """Replace literal occurrences of each secret in `text` with `REDACTED`.

    Defense in depth for text we don't fully control -- most commonly the
    `str()` of an exception raised by an underlying driver (e.g. DuckDB's
    postgres extension embeds the *raw* connection string, password
    included, in its own connection-failure message). Pass the credential(s)
    actually used to connect (a password, a token, ...) and any verbatim
    occurrence in `text` is stripped, regardless of where it came from.

    Empty/`None` secrets are ignored (so callers can pass an optional token
    without an `if` guard).
    """
    scrubbed = text
    for secret in secrets:
        if secret:
            scrubbed = scrubbed.replace(secret, REDACTED)
    return scrubbed
