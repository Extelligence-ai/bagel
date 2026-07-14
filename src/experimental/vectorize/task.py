"""EXPERIMENTAL: a pipeline task that indexes events as a reduce run detects them.

This is the corpus flywheel: point a pipeline's cadence at an event predicate, add this
task, and every firing records a searchable event in a local DuckDB index. On the edge the
index is a file on the robot (nothing leaves the box); for a fleet, point it at MotherDuck.

Not part of a release. APIs will change.
"""

import pathlib

import duckdb

from src import artifacts
from src.di import module
from src.experimental.vectorize.embedder import make_embedder
from src.experimental.vectorize.index import EventIndex
from src.experimental.vectorize.store import SemanticEventStore
from src.pipeline import base


class EmbedEventTask(base.Task):
    """Index each fired event into a semantic event store."""

    def __init__(  # noqa: PLR0913
        self,
        predicate: str,
        event_topic: str | None = None,
        index_path: str | None = None,
        embedder: str = "hashing",
        dim: int = 256,
        model_name: str = "all-MiniLM-L6-v2",
    ) -> None:
        """Initialize the task.

        Args:
            predicate (str): A human label for the event, e.g. "hard deceleration". Used as
                the description seed and stored as metadata for evaluation.
            event_topic (str | None): The topic the event was detected on.
            index_path (str | None): Path to a DuckDB file for the index (persists across
                runs, which is the point). If None, an in-memory index is used (per run).
            embedder (str): "hashing" (default) or "sentence-transformer" (local model).
            dim (int): Vector dimensionality for the hashing embedder.
            model_name (str): The sentence-transformers model id.

        """
        self._predicate = predicate
        self._event_topic = event_topic
        self._index_path = index_path
        self._embedder_kind = embedder
        self._dim = dim
        self._model_name = model_name
        self._store: SemanticEventStore | None = None

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """No data-source dependency; the task indexes event metadata directly."""

    def _get_store(self) -> SemanticEventStore:
        """Build (once) the semantic store, opening the persistent index if configured."""
        if self._store is None:
            embedder = make_embedder(
                self._embedder_kind, dim=self._dim, model_name=self._model_name
            )
            if self._index_path:
                pathlib.Path(self._index_path).parent.mkdir(parents=True, exist_ok=True)
                connection = duckdb.connect(self._index_path)
            else:
                connection = duckdb.connect()
            self._store = SemanticEventStore(embedder, EventIndex(embedder.dim, connection))
        return self._store

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        """Index the event fired at `asof_seconds`."""
        store = self._get_store()
        event_id = artifacts.short_digest(
            [self.pipeline, self.site, self.asset, self.log_id, f"{asof_seconds:.6f}"]
        )
        store.add_event(
            event_id,
            self._predicate,
            event_topic=self._event_topic,
            asset=self.asset,
            site=self.site,
            stats={"t_seconds": round(asof_seconds, 3)},
        )


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = EmbedEventTask
