# Resource & behavior knobs (env vars on the server container)

Defined in `settings.py`; set via the container environment or `.env`.

| Knob | Default | Meaning |
|---|---|---|
| `MCP_SERVER_PORT` | 8000 | MCP endpoint port (host mapping follows it in compose) |
| `MCP_SERVER_HOST` | `0.0.0.0` | In-container listen address; host-side exposure is governed by the compose port mapping - see SECURITY.md before sharing the endpoint beyond the machine |
| `MCP_TRANSPORT` | `sse` | MCP transport the server runs |
| `JSONL_BUFFER_SIZE_PER_TOPIC_BYTES` | 1 GB | Rolling live buffer per subscribed topic |
| `SINK_TOTAL_BUFFER_BYTES` | 0 (unbounded) | Total live-buffer budget per sink; when set, subscribe() refuses topics that would exceed it (`BufferCapacityExceededError` names both knobs to adjust) |
| `CACHE_MAX_BYTES` | 20 GB | LRU cap on the arrow query cache; 0 disables eviction. Never touches sink buffers or artifacts |
| `MAX_ARROW_RECORD_BATCH_SIZE_COUNT` | 100000 | Row ceiling per arrow batch; bounds peak memory when converting large topics |
| `ARTIFACT_DIRECTORY` | `~/.bagel/artifacts` | User deliverables (snippets, GIFs, exports). Never cleaned up automatically |
| `USER_CAPABILITIES_DIRECTORY` | `~/.bagel/capabilities` | User-authored capability files, discovered by `list_agent_capabilities`, written by `save_agent_capability`. Never auto-deleted |
| `CACHE_DIRECTORY` | `~/.cache/bagel` | Intermediate artifact cache |
| `STARTUP_PIPELINES_FILE` | unset | YAML manifest of subscriptions/pipelines re-established on boot |
| `ARROW_RECORD_BATCH_SIZE_BYTES` | 1 GB | Bytes per record batch in arrow files (best effort) |
| `MIN_ARROW_RECORD_BATCH_SIZE_COUNT` | 500 | Minimum records per arrow batch |
| `ROSBRIDGE_QUEUE_LENGTH` | 1000 | Messages buffered in rosbridge before sending |
| `CLOUDINI_ENABLED` | true | Cloudini pointcloud decompression in pipelines |
| `FLEET_ENABLED` | 1 | Kill switch for fleet streaming (beta). `0` makes the publish subsystem inert: nothing connects, nothing leaves the box |
| `BAGEL_FLEET` (build arg) | true | Compose build arg; `false` omits the MQTT client from the iot/ros2 images entirely |
