---
name: operating-live-sinks
description: Use when subscribing to live robot data (MQTT, rosbridge) through bagel, managing standing pipelines, or when buffer/cache errors and disk-usage questions come up (BufferCapacityExceededError, CACHE_MAX_BYTES, SINK_TOTAL_BUFFER_BYTES).
---

# Operating live sinks

Live data flows into per-topic disk buffers on the bagel server; standing
pipelines run against them.

- Subscribe/unsubscribe with the sink tools; each topic gets a rolling buffer
  (default 1 GB/topic, up to 2x on disk transiently during rotation).
- A `BufferCapacityExceededError` on subscribe is admission control, not a bug:
  the sink's total budget (`SINK_TOTAL_BUFFER_BYTES`) would be exceeded. Either
  lower the per-topic `buffer_size_bytes` or raise the budget — the error
  message names both knobs. See references/settings-knobs.md for every knob and
  its default.
- The arrow query cache self-limits (`CACHE_MAX_BYTES`, default 20 GB, LRU);
  artifacts under `~/.bagel/artifacts` are never auto-deleted — they are the
  user's deliverables.
- Standing pipelines that must survive container restarts belong in the
  `STARTUP_PIPELINES_FILE` manifest.
