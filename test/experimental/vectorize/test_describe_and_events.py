"""Tests for event descriptions, the local embedder guard, and add_event."""

import pytest

from src.experimental.vectorize.describe import describe_event
from src.experimental.vectorize.embedder import HashingEmbedder, SentenceTransformerEmbedder
from src.experimental.vectorize.store import SemanticEventStore


def test_describe_event_minimal() -> None:
    assert describe_event("hard brake") == "hard brake"


def test_describe_event_full_and_stable() -> None:
    text = describe_event(
        "hard deceleration",
        event_topic="/imu",
        asset="forklift_3",
        site="warehouse",
        stats={"peak_accel_x": -12.4, "duration_s": 2.1},
    )
    assert text == (
        "hard deceleration; on /imu on forklift_3 at warehouse; "
        "peak_accel_x -12.4, duration_s 2.1"
    )
    # deterministic
    assert text == describe_event(
        "hard deceleration",
        event_topic="/imu",
        asset="forklift_3",
        site="warehouse",
        stats={"peak_accel_x": -12.4, "duration_s": 2.1},
    )


def test_sentence_transformer_embedder_guarded_when_absent() -> None:
    # sentence-transformers is not a hard dependency; without it, the embedder must fail
    # with a clear, actionable error rather than a bare ImportError deep in the stack.
    with pytest.raises(ImportError, match="sentence-transformers"):
        SentenceTransformerEmbedder()


def test_add_event_indexes_with_predicate_metadata_and_is_searchable() -> None:
    store = SemanticEventStore(HashingEmbedder(dim=256))
    description = store.add_event(
        "evt-1",
        "hard deceleration forklift",
        event_topic="/imu",
        asset="forklift_3",
        stats={"peak": -12.4},
    )
    assert "hard deceleration forklift" in description
    assert store.index.count() == 1

    hit = store.search("hard deceleration forklift", k=1)[0]
    assert hit["event_id"] == "evt-1"
    assert hit["metadata"]["predicate"] == "hard deceleration forklift"
    assert hit["metadata"]["asset"] == "forklift_3"
