---
name: authoring-pipelines
description: Use when the user wants a bagel data pipeline — reducing a log to windows around events ("keep 30s around every hard brake"), recurring snippets/GIFs/exports, or any tasks-and-gates automation over robot data.
---

# Authoring bagel pipelines

Turn the user's request into a bagel pipeline config, preview it, and run it
only after the user confirms. Never run a pipeline the user has not seen
previewed.

1. **Describe the source.** Call `describe_data_source` on the path to list its
   topics, then `describe_topic` on the topic(s) the request refers to, to learn
   the field names, types and units. Don't guess field paths.
2. **Set the event.** The condition that marks something interesting ("decelerates
   harder than -10"), as a DuckDB SQL boolean predicate over the event topic.
   The topic column is a `STRUCT`, so nested fields are written
   `topic['field']`, for example `imu['linear_acceleration']['x'] < -10`. Take
   the field path and units from the topic description.
3. **Set the window.** How much data to keep around each event: `pre_seconds`
   before and `post_seconds` after. A single number ("30 seconds around") means
   both.
4. **Choose the output mode** from the user's wording:
   - **reduce**: one new bag holding only the union of event windows; everything
     else is discarded. For "only keep", "drop the rest", "reduce".
   - **snippet**: one clip per event. For "a clip for each event", "cut out each
     event".
5. **Set a debounce** (seconds) when the condition can stay true across many
   messages, so one burst counts as one event.
6. **Preview.** Call `preview_pipeline` with the path, event topic, predicate,
   `pre_seconds`, `post_seconds` and, if set, `debounce_seconds`. Nothing is
   written. Tell the user the event count and the fraction of data kept (for
   example "7 events, keeping 7.6% of the data").
7. **Write the config.** Call `list_pipeline_capabilities` for the exact task
   `module` path for this source format and its argument names; don't guess
   either. Fill in the matching template from `references/pipeline-templates.md`.
   Show the user the YAML in a fenced code block alongside the preview summary.
8. **Ask the user to confirm.** Keep the predicate and window exactly as
   previewed, so the run matches what they approved.
9. **Run it through the MCP tools** once confirmed: `run_pipeline` with the
   config, or `run_pipeline_batch` with the config and a list of paths to apply
   it to many sources. To keep the config for later, `save_pipeline` writes it
   as YAML. Don't shell out to the host CLI (`bagel-run`); that is for users
   working in a terminal, not plugin sessions.

If the bagel tools are missing, Bagel is not connected yet. The user either
registers the pip server (`claude mcp add bagel -- uvx --from "bagel-mcp[ros]"
bagel-mcp --transport stdio`; `codex mcp add bagel -- uvx --from
"bagel-mcp[ros]" bagel-mcp --transport stdio`) for recorded data (bags with the
`ros` extra, flight logs, MCAP, CAN/MDF, CSV), or starts the Docker container
for their data format (live ROS robots, fleet/edge) and, in Codex, connects it
once with `codex mcp add bagel --url http://localhost:8000/mcp`. See
references/formats.md for the format → extra / image → extra-args table.
Claude Code plugin installs connect to the Docker server automatically.
