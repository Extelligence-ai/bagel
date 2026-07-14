"""Tests for the experimental event vectorization slice."""

import math

import pytest

from src.experimental.vectorize.embedder import Embedder, HashingEmbedder
from src.experimental.vectorize.index import Event, EventIndex
from src.experimental.vectorize.store import SemanticEventStore


def test_hashing_embedder_is_deterministic_and_normalized() -> None:
    embedder = HashingEmbedder(dim=64)
    a = embedder.embed("hard deceleration on the forklift")
    b = embedder.embed("hard deceleration on the forklift")
    assert a == b  # deterministic
    assert len(a) == 64
    assert math.isclose(math.sqrt(sum(v * v for v in a)), 1.0, abs_tol=1e-6)  # unit length


def test_hashing_embedder_empty_text_is_zero_vector() -> None:
    assert HashingEmbedder(dim=16).embed("") == [0.0] * 16


def test_hashing_embedder_rejects_bad_dim() -> None:
    with pytest.raises(ValueError, match="positive"):
        HashingEmbedder(dim=0)


def test_hashing_embedder_satisfies_protocol() -> None:
    assert isinstance(HashingEmbedder(), Embedder)


def test_index_add_search_and_count() -> None:
    index = EventIndex(dim=3)
    index.add(Event("a", "x axis", [1.0, 0.0, 0.0]))
    index.add(Event("b", "y axis", [0.0, 1.0, 0.0]))
    assert index.count() == 2

    results = index.search([1.0, 0.0, 0.0], k=2)
    assert results[0]["event_id"] == "a"
    assert results[0]["score"] > results[1]["score"]


def test_index_insert_or_replace_by_id() -> None:
    index = EventIndex(dim=2)
    index.add(Event("a", "first", [1.0, 0.0]))
    index.add(Event("a", "second", [0.0, 1.0]))
    assert index.count() == 1
    assert index.search([0.0, 1.0], k=1)[0]["description"] == "second"


def test_index_rejects_dim_mismatch() -> None:
    index = EventIndex(dim=3)
    with pytest.raises(ValueError, match="dim"):
        index.add(Event("a", "bad", [1.0, 0.0]))
    with pytest.raises(ValueError, match="dim"):
        index.search([1.0, 0.0], k=1)


def test_semantic_store_finds_events_like_the_query() -> None:
    store = SemanticEventStore(HashingEmbedder(dim=256))
    store.add("e1", "hard deceleration forklift loading dock", {"asset": "forklift_3"})
    store.add("e2", "gentle turn warehouse aisle", {"asset": "forklift_3"})
    store.add("e3", "hard braking forklift stopped", {"asset": "forklift_7"})

    results = store.search("hard forklift", k=3)
    top_two = {results[0]["event_id"], results[1]["event_id"]}
    assert top_two == {"e1", "e3"}  # the shared-token events rank first
    assert results[2]["event_id"] == "e2"  # unrelated event ranks last
    assert results[2]["score"] < 1e-6  # orthogonal
    # metadata round-trips through the index
    assert results[0]["metadata"]["asset"] in {"forklift_3", "forklift_7"}
