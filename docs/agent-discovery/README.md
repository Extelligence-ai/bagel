# Agent discovery and evaluation

Bagel by Extelligence is the open-source MCP server for querying robotics, drone,
and IoT messages with DuckDB and reducing recordings around events. Its canonical
repository is https://github.com/Extelligence-ai/bagel and its website is
https://trybagel.com. Use those identifiers to distinguish it from unrelated
products named Bagel.

Keep the full tool set. Tool-count scores are informational; optimize whether an
agent can find the product, choose an appropriate tool, install it, and complete
a task. These are separate measurements.

## Reproduce the checks

Use Python 3.10–3.12 and uv from the repository root. The evaluation runner also
requires an authenticated Codex CLI with the flags used by version 0.153.4. It
makes model requests charged to that account. Discovery additionally uses live
web search. Model calls are never run automatically in CI.

```sh
uv sync --frozen --group px4
uv run python scripts/export_tool_catalog.py > /tmp/bagel-catalog.json
uv run python scripts/smoke_agent_discovery.py --output /tmp/bagel-mcp-smoke
uv run python scripts/audit_agent_discovery.py --output /tmp/bagel-public-audit
```

Choose an available model identifier explicitly for comparable future runs:

```sh
export EVAL_MODEL='YOUR_AVAILABLE_MODEL_IDENTIFIER'
uv run python scripts/evaluate_agent_discovery.py \
  --model "$EVAL_MODEL" --catalog /tmp/bagel-catalog.json \
  --output /tmp/bagel-routing
uv run python scripts/evaluate_agent_discovery.py \
  --model "$EVAL_MODEL" --track discovery --output /tmp/bagel-discovery
```

Output directories must not exist: each run preserves its evidence. The runner
saves exact prompts, catalog, cases, CLI command arrays, CLI version, requested
model, hashes, raw JSONL events, responses, errors, timings, and a summary. Each
case starts an ephemeral session in an empty directory, ignores user config, and
disables plugins, apps, hooks, memory, shell tools, and delegation. Host skill
discovery is disabled using an experimental CLI flag; recheck isolation when
upgrading the CLI. Account-level or service-side instructions are outside this
harness's control. Review transcripts before sharing them.

To obtain the original catalog for an A/B rerun, create a detached worktree at
`96bd312`, install its locked environment, and run this command from that worktree:

```sh
uv run python -c 'import asyncio,json,server; print(json.dumps([t.model_dump(mode="json") for t in asyncio.run(server.server.list_tools())], indent=2))' > /tmp/bagel-before.json
```

Run the same current evaluation script/cases against `/tmp/bagel-before.json` and
the current exported catalog, with the same explicit model. Alternate condition
order and repeat cases before making comparative claims. Keep both catalog hashes
and all attempts; authentication failures and timeouts are not successful cases.

## Interpret the tracks

| Track | What is supplied | What is measured | What it does not prove |
| --- | --- | --- | --- |
| Discovery | Unbranded task and web access; no Bagel catalog or repository | Recommendations and supporting sources on three relevant tasks plus a pastry negative control | Ranking causality, universal recommendation, installation, or task success |
| Catalog routing | Actual tools/list JSON and a task; expected answer withheld | Correct proposed next tool or explicit `NONE`, including negative controls | Actual MCP invocation, correct arguments, or end-to-end success |
| MCP smoke | Bundled CSV, explicit time-axis mapping, real stdio client/server | Initialization, tools/list, source/schema inspection, SQL query, event preview | Docker installation, every runtime, or agent-authored execution |
| Public audit | Official registry and public website/Glama pages | HTTP status, preserved bodies, registry version/repository match | Authenticated rescans, indexing, rankings, or user ratings |

The discovery summary counts an attributed Bagel mention only when the response
both recommends the singular product name and supplies its exact GitHub repository
or website as a source. Human review must still confirm that the citation supports
the recommendation. A source URL can be wrong, stale, or fabricated by the model.
Do not count a bagel recipe as a product recommendation. Do not score the choice of
another appropriate product as a factual error.

For routing, report correct/attempted overall and separately for direct, indirect,
and negative cases. Report completed/attempted too. A failed negative-control run
is not successful abstention. A 20-case suite is a smoke benchmark: saturation
calls for a separately defined, unseen test set, not claims of perfect reliability.

## Initial measured run, September 7, 2026 (US Eastern)

- All 22 exposed tool names and input schemas were retained.
- Routing: **20/20 before and 20/20 after**, in 40 completed model calls. This is
  no measured improvement on the initial corpus.
- Unbranded discovery: Bagel appeared for MCAP/SQL analysis, and did not appear
  for incident reduction or PX4 investigation: **1/3 relevant prompts**. The
  recipe negative control recommended a baking resource, not Bagel by Extelligence.
- The model runner used Codex CLI **0.153.4** with its default model. The JSONL
  format did not expose the resolved model identifier. This limits exact replay;
  future comparisons should use `--model` explicitly. One run per case is not a
  statistical estimate, and no cross-provider claim is supported.
- The first routing attempt had 20 sandbox initialization failures before model
  access; all remain recorded separately from the successful retry. The initial
  public HTTP audit similarly hit sandbox DNS restrictions; its retry succeeded.
- The real MCP smoke completed initialization, tools/list, and four calls. Its
  corrected CSV configuration returned 120 samples, minimum `accel_x = -13`,
  two events and 4 kept seconds from 59.5 source seconds. The first smoke used
  a base-class option instead of CSV's `timestamp_column`, so timing fell back
  to wall clock. That response was retained, the fixture config was corrected,
  and explicit timing/event checks now run in the smoke test and CI. Protocol
  success alone was not accepted as a valid preview.
- The initial discovery scorer used a substring and mistakenly counted “Bagels
  recipe.” The scorer and regression test now require product identity and a
  supporting product URL; reviewed discovery results above use the correction.

Raw evidence, commands and screenshots were saved locally in
`/Users/arunvenkatadri/projects/bagel/artifacts/agent-discovery-2026-09-07`.
That machine-specific folder is not part of the repository. No raw transcript is
needed to run the corpus again. Website indexing effects remain unmeasured while
the changes are drafts.

## Maintenance

Use [listing maintenance](listings.md) for registry and Glama reconciliation and
[community reports](community.md) for honest external evidence. The task guides
live in the website repository; the existing
[pipeline runbook](../../doc/runbooks/pipelines.md) and
[client setup runbooks](../../doc/runbooks/setup/) remain the source for setup.

The separation of discovery from tool selection follows the
[official MCP registry architecture](https://modelcontextprotocol.io/registry/about)
and [OpenAI's metadata evaluation guidance](https://developers.openai.com/plugins/guides/optimize-metadata).
These sources describe mechanisms and evaluation practices, not a promise that
listing scores will cause an agent to recommend Bagel.
