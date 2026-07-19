"""A data source factory for InfluxDB 3 databases.

Connects via `influxdb3-python` (Arrow Flight SQL), which returns PyArrow tables --
a direct fit for Bagel's Arrow/DuckDB core. Each measurement (table) is a Bagel
"topic"; InfluxDB 3's mandatory `time` column provides timestamps.

The data source `path` is a URL of the form::

    influxdb://<token>@<host>:<port>/<database>

The token may instead be passed via `args` (``{"token": "..."}``), which takes
precedence over the URL. The default port is 8181 (InfluxDB 3 Core). Targets
InfluxDB 3 (Core/Enterprise/Cloud Dedicated); v1/v2 are in maintenance upstream.
"""

import functools
import uuid
from typing import Any
from urllib.parse import unquote, urlparse

from influxdb_client_3 import InfluxDBClient3
from pydantic import BaseModel

from bagel.di import module

DEFAULT_PORT = 8181  # InfluxDB 3 Core


def parse_url(url: str) -> dict[str, str]:
    """Parse an ``influxdb://token@host:port/database`` URL.

    Returns:
        A dict with ``host`` (scheme://host:port for the client), ``database``,
        and ``token`` (possibly empty).

    Raises:
        ValueError: If the URL has no host or no database path segment.

    """
    parsed = urlparse(url)
    if parsed.scheme != "influxdb":
        raise ValueError(f"Expected an influxdb:// URL, got: {url}")
    if not parsed.hostname:
        raise ValueError(f"Missing host in InfluxDB URL: {url}")
    database = parsed.path.lstrip("/")
    if not database:
        raise ValueError(
            f"Missing database in InfluxDB URL: {url} "
            "(expected influxdb://token@host:port/database)"
        )
    port = parsed.port or DEFAULT_PORT
    return {
        "host": f"http://{parsed.hostname}:{port}",
        "database": database,
        "token": unquote(parsed.username) if parsed.username else "",
    }


@functools.lru_cache
def client(host: str, database: str, token: str) -> InfluxDBClient3:
    """Return a cached InfluxDB client for the given connection parameters."""
    return InfluxDBClient3(host=host, database=database, token=token)


class InfluxDatabase(BaseModel):
    """Represent an InfluxDB 3 data source."""

    host: str
    database: str
    token: str = ""

    @property
    def client(self) -> InfluxDBClient3:
        """The (cached) InfluxDB client."""
        return client(self.host, self.database, self.token)

    def tables(self) -> list[str]:
        """Return the measurement (table) names in the database."""
        result = self.client.query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'iox' ORDER BY table_name"
        )
        return result["table_name"].to_pylist()

    def __hash__(self) -> int:
        """Needed for functools caching."""
        return hash((self.host, self.database, self.token))


class SourceFactory:
    """A data source factory for InfluxDB 3 databases."""

    def __init__(self, path: str, token: str | None = None) -> None:
        """Initialize the InfluxDB data source factory.

        Args:
            path (str): The connection URL, ``influxdb://<token>@<host>:<port>/<database>``.
            token (str | None, optional): Authentication token; overrides any token in
                the URL. Defaults to None.

        Raises:
            ValueError: If the URL is malformed.
            ConnectionError: If the database cannot be reached.

        """
        parts = parse_url(path)
        self._url = path
        self._database = InfluxDatabase(
            host=parts["host"],
            database=parts["database"],
            token=token or parts["token"],
        )
        try:
            self._database.tables()
        except Exception as error:
            raise ConnectionError(f"Cannot connect to '{parts['host']}': {error}") from error

    @property
    def path(self) -> str:
        """The connection URL."""
        return self._url

    @property
    def uuid(self) -> str:
        """A unique identifier for the data source."""
        return str(uuid.uuid5(uuid.NAMESPACE_URL, self._url))

    def build(self) -> InfluxDatabase:
        """Return an InfluxDatabase object."""
        return self._database

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the database: measurements and their row counts."""
        database = self.build()
        tables = []
        for table in database.tables():
            count_result = database.client.query(
                f'SELECT COUNT(*) AS n FROM "{table}"'  # noqa: S608
            )
            tables.append({"topic": table, "row_count": count_result["n"][0].as_py()})
        return {"host": database.host, "database": database.database, "tables": tables}


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
