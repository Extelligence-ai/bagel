---
name: authoring-pipelines
description: Use when the user wants a bagel data pipeline — reducing a log to windows around events ("keep 30s around every hard brake"), recurring snippets/GIFs/exports, or any tasks-and-gates automation over robot data.
---

# Authoring bagel pipelines

The bagel MCP server owns the authoring workflow, including the
reduce-vs-snippet decision, window/debounce extraction, and the
preview-before-run rule. Do not write pipeline YAML from memory:

1. Call `run_poml_capability` with `poml_path="./bagel_mcp/agent/compose/pipeline.poml"`.
2. Follow it exactly. In particular: always call `preview_pipeline` and show the
   user the summary (events found, data kept) BEFORE running anything.
3. Use `list_pipeline_capabilities` for the exact task/gate module paths and
   arguments — never guess them.
4. Execute the approved config through the MCP tools (`run_pipeline`, or
   `run_pipeline_batch` for many sources). The capability also mentions a host
   CLI (`bagel-run`); that path is for users at a terminal in the repo, not for
   plugin sessions — do not shell out to it.

If the bagel tools are missing, Bagel is not connected yet. The user either
registers the pip server (`claude mcp add bagel -- uvx bagel-mcp --transport
stdio`; `codex mcp add bagel -- uvx bagel-mcp --transport stdio`) for flight
logs, MCAP, CAN/MDF and CSV, or starts the Docker container for their data
format (ROS `.db3`/`.bag` bags, live robots) and, in Codex, connects it once
with `codex mcp add bagel --url http://localhost:8000/mcp`. See
references/formats.md for the format → extra / image → extra-args table.
Claude Code plugin installs connect to the Docker server automatically.
