"""Tests for the EmbedEventTask flywheel and the predicate-based eval."""

import pathlib

from src.experimental.vectorize.embedder import HashingEmbedder
from src.experimental.vectorize.eval import precision_at_k_by_predicate
from src.experimental.vectorize.store import SemanticEventStore
from src.experimental.vectorize.task import EmbedEventTask


def _configure(task: EmbedEventTask, asset: str = "forklift_3") -> EmbedEventTask:
    """Set the operator attributes Operator.build would normally set."""
    task._pipeline = "reduce_and_index"
    task._name = "embed_event"
    task._site = "warehouse"
    task._asset = asset
    task._log_id = "log-1"
    task._path = "/data/bag"
    return task


def test_task_indexes_each_fired_event() -> None:
    task = _configure(EmbedEventTask(predicate="hard deceleration", event_topic="/imu", dim=128))
    task._store = SemanticEventStore(HashingEmbedder(dim=128))  # in-memory, injected

    task.execute(asof_seconds=12.5, lookback=None)
    task.execute(asof_seconds=40.0, lookback=None)

    assert task._store.index.count() == 2
    hit = task._store.search("hard deceleration forklift", k=1)[0]
    assert hit["metadata"]["predicate"] == "hard deceleration"
    assert hit["metadata"]["asset"] == "forklift_3"


def test_task_dedupes_same_event_id() -> None:
    task = _configure(EmbedEventTask(predicate="hard brake", dim=64))
    task._store = SemanticEventStore(HashingEmbedder(dim=64))
    task.execute(asof_seconds=5.0, lookback=None)
    task.execute(asof_seconds=5.0, lookback=None)  # same asof -> same id -> replace
    assert task._store.index.count() == 1


def test_task_persists_to_a_duckdb_file(tmp_path: pathlib.Path) -> None:
    index_path = str(tmp_path / "nested" / "events.duckdb")
    task = _configure(EmbedEventTask(predicate="hard brake", index_path=index_path, dim=64))
    task.execute(asof_seconds=1.0, lookback=None)
    assert pathlib.Path(index_path).exists()
    assert task._get_store().index.count() == 1


def test_precision_at_k_separates_predicates() -> None:
    store = SemanticEventStore(HashingEmbedder(dim=256))
    # two predicates, two events each (varying asset so descriptions are not identical)
    store.add_event("b1", "hard deceleration", asset="a")
    store.add_event("b2", "hard deceleration", asset="b")
    store.add_event("t1", "gentle turn", asset="a")
    store.add_event("t2", "gentle turn", asset="b")

    score = precision_at_k_by_predicate(store.index, k=1)
    # each event's nearest neighbor should be its same-predicate twin
    assert score == 1.0


def test_precision_returns_zero_for_trivial_index() -> None:
    store = SemanticEventStore(HashingEmbedder(dim=16))
    assert precision_at_k_by_predicate(store.index, k=5) == 0.0
    store.add_event("only", "lonely event")
    assert precision_at_k_by_predicate(store.index, k=5) == 0.0
