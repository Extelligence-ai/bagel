"""EXPERIMENTAL: a semantic store over reduced events (embedder + index facade).

Compose an `Embedder` with an `EventIndex` so callers work in natural language: add an
event by its description, search by a text query. The reduced set is small enough to
vectorize, which is the whole point -- you could not do this to the raw sensor firehose.

Not part of a release. APIs will change.
"""

from typing import Any

from src.experimental.vectorize.describe import describe_event
from src.experimental.vectorize.embedder import Embedder
from src.experimental.vectorize.index import Event, EventIndex


class SemanticEventStore:
    """Add and search reduced events in natural language, backed by a vector index."""

    def __init__(self, embedder: Embedder, index: EventIndex | None = None) -> None:
        """Initialize the store.

        Args:
            embedder (Embedder): Turns text into vectors.
            index (EventIndex | None): The vector index to use. If None, an in-memory
                index sized to the embedder is created.

        """
        self._embedder = embedder
        self._index = index or EventIndex(embedder.dim)

    @property
    def index(self) -> EventIndex:
        """The underlying vector index."""
        return self._index

    def add(
        self, event_id: str, description: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Embed a description and index the event under `event_id`."""
        embedding = self._embedder.embed(description)
        self._index.add(
            Event(
                event_id=event_id,
                description=description,
                embedding=embedding,
                metadata=metadata or {},
            )
        )

    def add_event(  # noqa: PLR0913
        self,
        event_id: str,
        predicate: str,
        *,
        event_topic: str | None = None,
        asset: str | None = None,
        site: str | None = None,
        stats: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Describe a reduced event, embed the description, and index it.

        Builds a deterministic description from the predicate and context, then stores it.
        The predicate is kept in metadata so retrieval can be evaluated by predicate later.

        Returns:
            The generated description (useful for inspection and eval).

        """
        description = describe_event(
            predicate, event_topic=event_topic, asset=asset, site=site, stats=stats
        )
        full_metadata = {"predicate": predicate, **(metadata or {})}
        for key, value in (("event_topic", event_topic), ("asset", asset), ("site", site)):
            if value is not None:
                full_metadata.setdefault(key, value)
        self.add(event_id, description, metadata=full_metadata)
        return description

    def search(self, query: str, k: int = 5) -> list[dict[str, Any]]:
        """Embed a natural-language query and return the top-k most similar events."""
        return self._index.search(self._embedder.embed(query), k)
