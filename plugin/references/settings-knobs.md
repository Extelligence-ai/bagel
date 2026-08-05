# Resource & behavior knobs (env vars on the server container)

| Knob | Default | Meaning |
|---|---|---|
| `MCP_SERVER_PORT` | 8000 | MCP endpoint port |
| `MCP_TRANSPORT` | `sse` | `sse` or `streamable-http` |
| `JSONL_BUFFER_SIZE_PER_TOPIC_BYTES` | 1 GB | Rolling live buffer per topic; on-disk can transiently reach 2x during rotation |
| `SINK_TOTAL_BUFFER_BYTES` | 0 (unbounded) | Total live-buffer budget per sink; when set, subscribe() refuses topics that would exceed it (`BufferCapacityExceededError`) |
| `CACHE_MAX_BYTES` | 20 GB | LRU cap on the arrow query cache; 0 disables eviction. Never touches sink buffers or artifacts |
| `ARTIFACT_DIRECTORY` | `~/.bagel/artifacts` | User deliverables (snippets, GIFs, exports). Never auto-deleted; `datestr=` partitions make external rotation easy |
| `USER_CAPABILITIES_DIRECTORY` | `~/.bagel/capabilities` | User-authored capability files, discovered by `list_agent_capabilities`, written by `save_agent_capability`. Never auto-deleted |
| `CACHE_DIRECTORY` | `~/.cache/bagel` | Query cache + sink buffers + repo clones |
| `STARTUP_PIPELINES_FILE` | unset | YAML manifest of subscriptions/pipelines re-established on boot |

Interpreting errors: `BufferCapacityExceededError` names the two knobs to
adjust; a PyArrow source raising "excluded as invalid" means every candidate
file failed the format check (bad file, not empty data).
