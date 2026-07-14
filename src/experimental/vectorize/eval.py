"""EXPERIMENTAL: a quick retrieval-quality metric using predicates as weak labels.

Every reduced event carries the trigger predicate for free, so we can measure retrieval
without hand labeling: an event's nearest neighbors should mostly share its predicate. This
gives a single "is search working" number to watch as embedders improve.

Not part of a release. APIs will change.
"""

from src.experimental.vectorize.index import EventIndex

_MIN_EVENTS_FOR_EVAL = 2


def precision_at_k_by_predicate(index: EventIndex, k: int = 5) -> float:
    """Return the mean fraction of each event's top-k neighbors that share its predicate.

    For every indexed event, search with its own embedding, drop itself, and measure how
    many of the k nearest neighbors have the same `predicate` in their metadata. Averaged
    over all events. 1.0 means events cluster perfectly by predicate; near 0 means the
    embedder is not separating them.

    Returns 0.0 if there are fewer than two events.
    """
    records = index.records()
    if len(records) < _MIN_EVENTS_FOR_EVAL:
        return 0.0

    total = 0.0
    for record in records:
        own_predicate = (record["metadata"] or {}).get("predicate")
        results = index.search(record["embedding"], k=k + 1)
        neighbors = [hit for hit in results if hit["event_id"] != record["event_id"]][:k]
        if not neighbors:
            continue
        matches = sum(
            1 for hit in neighbors if (hit["metadata"] or {}).get("predicate") == own_predicate
        )
        total += matches / len(neighbors)

    return total / len(records)
