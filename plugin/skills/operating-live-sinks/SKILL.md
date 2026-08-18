---
name: operating-live-sinks
description: Use when subscribing to live robot data (MQTT, rosbridge) through bagel, managing standing pipelines that survive restarts, or when questions about live-buffer sizes and disk usage come up.
---

# Operating live sinks

Live data flows into per-topic rolling disk buffers on the bagel server;
standing pipelines run against them.

- Discover what a live source publishes with `list_live_topics`, then
  `subscribe_live_topics` to start recording; each topic gets a rolling buffer
  (`JSONL_BUFFER_SIZE_PER_TOPIC_BYTES`, default 1 GB per topic).
- The subscription's directory path is returned; pass it as the `path` for
  queries and pipelines over the live data.
- Standing pipelines that must survive container restarts belong in the
  `STARTUP_PIPELINES_FILE` manifest: they are re-established on boot.
- Artifacts (snippets, exports) land under the artifacts directory and are
  never cleaned up automatically: they are the user's deliverables. See
  references/settings-knobs.md for every knob and its default.
