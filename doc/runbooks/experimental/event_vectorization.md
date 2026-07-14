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
- `describe_event`: turns a reduced event into a deterministic text description from its
  predicate, location, and stats. No LLM call.
- `SemanticEventStore`: the facade. `add_event` describes, embeds, and indexes an event;
  `search` finds the closest ones for a text query.

```python
from src.experimental.vectorize.embedder import HashingEmbedder  # or SentenceTransformerEmbedder
from src.experimental.vectorize.store import SemanticEventStore

store = SemanticEventStore(HashingEmbedder(dim=256))
store.add_event("evt-1", "hard deceleration", event_topic="/imu", asset="forklift_3",
                stats={"peak_accel_x": -12.4, "duration_s": 2.1})
store.add_event("evt-2", "gentle turn", event_topic="/imu", asset="forklift_3")

for hit in store.search("hard braking forklift", k=3):
    print(hit["score"], hit["event_id"], hit["metadata"])
```

For real semantic search on the edge, swap in the local model (nothing leaves the box):

```python
from src.experimental.vectorize.embedder import SentenceTransformerEmbedder  # pip install sentence-transformers
store = SemanticEventStore(SentenceTransformerEmbedder())  # all-MiniLM-L6-v2, CPU
```

## Edge first

The whole loop is designed to run on the robot's companion computer, not in the cloud:

- Reduction already made the data sparse, so you embed the handful of events, not the
  firehose. A small sentence model (all-MiniLM-L6-v2) does that on CPU in milliseconds.
- The index is a local DuckDB file. Searching one robot's own history happens on the device,
  and no data leaves the box.

Only the **fleet** layer needs the cloud: to search across many robots you sync the tiny
event vectors and metadata (not the raw data) to a shared index (point `EventIndex` at
MotherDuck instead of a local file). Same code, two deployments, and it is the natural
free-edge / paid-cloud boundary.

Note: "edge" here means a companion computer or a Jetson-class board, not a microcontroller.

## Roadmap

- **Phase 0, plumbing (done):** `Embedder` protocol, DuckDB `EventIndex`, `SemanticEventStore`.
- **Phase 1, text search (in progress):** done so far: `describe_event`, `add_event`, a local
  `SentenceTransformerEmbedder`, the `EmbedEventTask` flywheel (every reduce firing indexes an
  event into a local DuckDB file), and `precision_at_k_by_predicate` (a retrieval-quality
  number that uses predicates as weak labels). Next: a `search_events` MCP tool so it is
  usable from chat.
- **Phase 2, richer descriptions:** optional LLM-generated window summaries.
- **Phase 3, signal matching:** a `WaveformEmbedder` (z-normalized, resampled window) into
  the same index for shape similarity, classical and training-free (MASS / matrix profile
  family); optional DTW for time-warp robustness.
- **Phase 4, fusion and surfaces:** combine text and signal, then RAG, novelty, and
  training-set curation. Swap brute-force cosine for DuckDB VSS/HNSW when the corpus is large.

## Honest limits

- The hashing embedder matches shared words, not meaning. It exists to validate the plumbing.
- Signal embeddings give morphological similarity; pair them with the event's context
  (predicate, description, magnitude) for semantic similarity.
- Cross-platform fleets (forklift vs drone vs quadruped) do not share a signal space; lean on
  the text embedding to bridge them, and keep signal similarity within an asset class.
