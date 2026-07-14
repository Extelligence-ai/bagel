"""EXPERIMENTAL: a DuckDB-backed vector index over reduced events.

Keeps event embeddings next to the SQL you already run. Search is brute-force cosine via
DuckDB's native `array_cosine_similarity`; swap in the VSS (HNSW) extension once the corpus
is large. Not part of a release. APIs will change.
"""

import json
from dataclasses import dataclass, field
from typing import Any

import duckdb


@dataclass
class Event:
    """A reduced event to index: an id, a description, its embedding, and metadata."""

    event_id: str
    description: str
    embedding: list[float]
    metadata: dict[str, Any] = field(default_factory=dict)


class EventIndex:
    """Store event embeddings in DuckDB and search them by cosine similarity."""

    def __init__(self, dim: int, connection: duckdb.DuckDBPyConnection | None = None) -> None:
        """Initialize the index.

        Args:
            dim (int): Embedding dimensionality. All added and queried vectors must match.
            connection (duckdb.DuckDBPyConnection | None): An existing DuckDB connection to
                use (e.g. a MotherDuck connection for a shared fleet index). If None, an
                in-memory connection is created.

        """
        self._dim = dim
        self._connection = connection or duckdb.connect()
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR PRIMARY KEY,
                description VARCHAR,
                metadata JSON,
                embedding FLOAT[{dim}]
            )
            """
        )

    @property
    def dim(self) -> int:
        """The embedding dimensionality of this index."""
        return self._dim

    def add(self, event: Event) -> None:
        """Insert or replace an event by its id.

        Raises:
            ValueError: If the embedding dimensionality does not match the index.

        """
        if len(event.embedding) != self._dim:
            raise ValueError(
                f"embedding dim {len(event.embedding)} != index dim {self._dim}"
            )
        self._connection.execute(
            "INSERT OR REPLACE INTO events VALUES (?, ?, ?, ?)",
            [event.event_id, event.description, json.dumps(event.metadata), event.embedding],
        )

    def search(self, embedding: list[float], k: int = 5) -> list[dict[str, Any]]:
        """Return the top-k most similar events by cosine similarity.

        Args:
            embedding (list[float]): The query vector.
            k (int): The number of results to return.

        Returns:
            A list of dicts with `event_id`, `description`, `metadata`, and `score`
            (cosine similarity in [-1, 1]), ordered by score descending.

        Raises:
            ValueError: If the query dimensionality does not match the index.

        """
        if len(embedding) != self._dim:
            raise ValueError(f"query dim {len(embedding)} != index dim {self._dim}")
        rows = self._connection.execute(
            f"""
            SELECT event_id, description, metadata,
                   array_cosine_similarity(embedding, ?::FLOAT[{self._dim}]) AS score
            FROM events
            ORDER BY score DESC
            LIMIT ?
            """,  # noqa: S608 -- self._dim is an internal int, not user input
            [embedding, k],
        ).fetchall()
        return [
            {
                "event_id": row[0],
                "description": row[1],
                "metadata": json.loads(row[2]),
                "score": row[3],
            }
            for row in rows
        ]

    def count(self) -> int:
        """Return the number of indexed events."""
        return self._connection.execute("SELECT count(*) FROM events").fetchone()[0]
