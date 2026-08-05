---
name: authoring-pipelines
description: Use when the user wants a bagel data pipeline — reducing a log to windows around events ("keep 30s around every hard brake"), recurring snippets/GIFs/exports, or any tasks-and-gates automation over robot data.
---

# Authoring bagel pipelines

The bagel MCP server owns the authoring workflow, including the
reduce-vs-snippet decision, window/debounce extraction, and the
preview-before-run rule. Do not write pipeline YAML from memory:

1. Call `run_poml_capability` with `poml_path="./src/agent/compose/pipeline.poml"`.
2. Follow it exactly. In particular: always call `preview_pipeline` and show the
   user the summary (events found, data kept) BEFORE running anything.
3. Use `list_pipeline_capabilities` for the exact task/gate module paths and
   arguments — never guess them.

If the connection fails, the server container is not running — see
${CLAUDE_PLUGIN_ROOT}/references/formats.md.
