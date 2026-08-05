---
name: triaging-robot-logs
description: Use when investigating a robot log or flight log with bagel — finding events, anomalies, or "what happened around X" in rosbags, MCAP, CAN, MDF4, ULog, or Betaflight files. Routes to the server's triage capability.
---

# Triaging robot logs

The bagel MCP server ships its own step-by-step triage workflow. Do not
improvise one: fetch and follow it.

1. Call `run_poml_capability` with `poml_path="./src/agent/triage/investigate.poml"`.
2. Follow the returned instructions exactly (describe-first, windowed queries,
   snippet/GIF evidence).

If the bagel tools are missing or the connection fails, the server is probably
not running — the user must start the Docker container for their data format
first (see ${CLAUDE_PLUGIN_ROOT}/references/formats.md for the format → image → extra-args table,
including the CAN `dbc` requirement).
