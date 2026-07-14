# Event vectorization (experimental)

> [!WARNING]
> Experimental. Developed on the `claude/experimental-event-vectorization` branch, not part
> of any release. The API will change and there are no compatibility guarantees.

## The idea

You cannot vectorize the raw sensor firehose from a robot fleet. It is too large. But once
Bagel has reduced a log to its event windows (the small fraction that matters), that reduced
set **is** small enough to embed. So reduction is not only a storage win, it is what makes
semantic search over physical-AI data tractable.

With events embedded, you can:

- Find events similar to a given one ("show me every near-collision like this").
- Search a whole fleet's history in natural language.
- Cluster failure modes and surface the rare, novel events (the ones worth reviewing or
  training on).

## What this slice does today

A minimal, dependency-free loop so the idea can be exercised end to end:

- `HashingEmbedder`: a deterministic text embedder (no ML dependencies). It is enough to
  prove the loop; it is not a semantic model.
- `EventIndex`: stores event embeddings in DuckDB and searches them with the native
  `array_cosine_similarity`, so the vectors sit next to the SQL you already run. A shared
  MotherDuck connection turns this into a fleet-wide index.
- `SemanticEventStore`: the facade. Add an event by its description, search by a text query.

```python
from src.experimental.vectorize.embedder import HashingEmbedder
from src.experimental.vectorize.store import SemanticEventStore

store = SemanticEventStore(HashingEmbedder(dim=256))
store.add("evt-1", "hard deceleration, forklift, loading dock", {"asset": "forklift_3"})
store.add("evt-2", "gentle turn in the warehouse aisle", {"asset": "forklift_3"})

for hit in store.search("hard braking forklift", k=3):
    print(hit["score"], hit["event_id"], hit["metadata"])
```

## Where it goes next

- **Real text embeddings**: drop a sentence encoder or an embedding API in behind the
  `Embedder` protocol. Bridges modalities and heterogeneous fleets, because language is the
  common space.
- **Signal and vision embeddings (the moat)**: embed the proprioceptive window (a time-series
  encoder) or camera keyframes (a vision encoder) for true "events that look like this."
  Each reduced event already ships with the trigger predicate and the topic schema, which
  are free weak labels and physical grounding.
- **VSS / HNSW**: swap brute-force cosine for DuckDB's vector index once the corpus is large.

## Honest limits

- The hashing embedder matches shared words, not meaning. It exists to validate the plumbing.
- Signal embeddings give morphological similarity; pair them with the event's context
  (predicate, description, magnitude) for semantic similarity.
- Cross-platform fleets (forklift vs drone vs quadruped) do not share a signal space; lean on
  the text embedding to bridge them, and keep signal similarity within an asset class.
