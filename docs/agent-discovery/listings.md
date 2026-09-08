# Listing maintenance

Canonical identity: **Bagel by Extelligence**. Repository:
https://github.com/Extelligence-ai/bagel. Registry name:
`io.github.Extelligence-ai/bagel`. Website: https://trybagel.com.
Keep the existing tool names and full functionality; tool count is not a target.

## Observed public state

The September 7, 2026 US Eastern audit found the official registry's latest entry
at **2.2.3**, matching `server.json` and the repository URL. It listed versions
2.0.1 through 2.2.3 with no next-page cursor. The public Glama page showed an
**A / 3.7 out of 5** tool-definition assessment discussing 18 tools, while current
Bagel exported 22. Its displayed repository file links referenced
`2697735ac80f00d754e3e9415b80fa6fefd53b0e`; those links do not establish the
assessment's exact code revision. Source: [Bagel's Glama listing](https://glama.ai/mcp/servers/Extelligence-ai/bagel).

Some completeness comments overlap features now present: `list_pipelines`,
`delete_pipeline`, and `delete_capability`, plus `save_agent_capability`'s overwrite
option. The live subscription API still lacks a dedicated stop/unsubscribe tool.
Document that limitation; do not imply that a rescan implements it.

The public website returned 404 for robots.txt and sitemap.xml. This is not proof
of blocked crawling. The website draft adds these files and task pages; CDN/WAF
behavior and actual search indexing must be checked after deployment.

## Release and rescan checklist

1. Run `scripts/audit_agent_discovery.py` and retain its report. Resolve network
   errors before treating a missing response as a missing listing. Check registry
   pagination, repository identity, version, package tag, and transport.
2. Export the current catalog with `scripts/export_tool_catalog.py`; compare all
   tool names, schemas, descriptions, annotations, and runtime availability.
3. Review the metadata PR and release through the normal tested pipeline. A
   published semver image tag is immutable: use a new version for a new image.
   Keep `pyproject.toml`, image tags and `server.json` aligned. Do not republish
   changed metadata under an already published immutable release.
4. In the maintainer's Glama account, verify the tracked repository/revision and
   request the supported rebuild/rescan after the intended code is available.
   Compare the resulting tool list and assessment with the exported catalog.
   The public API rejected unauthenticated access during this audit; use an
   account-owned API key only if the documented operation requires it.
5. Verify the registry and installed client plugin point to the intended release.
   Test the actual client handshake: SSE at `/sse`, streamable HTTP at `/mcp`.
   Codex uses `/mcp`; Claude Code's documented SSE command uses `/sse`.
6. After website deployment, fetch the canonical pages, robots.txt and sitemap;
   inspect redirects and any `X-Robots-Tag` headers, then use the owner's search
   console to submit/inspect the sitemap. Repeat the unbranded discovery corpus
   after indexing, retaining dates, sources and all outcomes.

The included weekly/manual GitHub Actions audit only fetches public surfaces and
uploads evidence. It neither changes listings nor runs paid model evaluations.
HTTP errors are recorded in the report for review; workflow success means the
report was collected, not that the surfaces are healthy.

## Prepared maintainer rescan note — not sent

> Bagel's current catalog contains 22 tools. The public tool-definition assessment
> discusses 18 and mentions missing saved-pipeline and capability lifecycle tools
> that current source now exposes. Please rebuild the intended released revision
> and compare the attached tools/list JSON. We are intentionally retaining the
> full tool set. We have clarified routing, prerequisites, output types and side
> effects; please assess those descriptions without assuming a tool-count target.
> A dedicated live unsubscribe tool is still absent and is documented as such.

Attach the exact release/commit, catalog JSON, build output and this listing URL.
Fill in the released revision when one exists; do not claim this draft was scanned.

## Crawl policy and indexes

The website draft explicitly permits OAI-SearchBot and ordinary crawling, and
provides a canonical sitemap. Search crawling and model training controls are
separate; the addition does not establish or alter a separate training policy.
See [OpenAI's crawler documentation](https://developers.openai.com/api/docs/bots).
A small llms.txt is a convenience index, not a verified ranking factor.

Glama's [methodology](https://glama.ai/mcp/methodology) explains its build and tool
assessment process. The [official MCP registry](https://modelcontextprotocol.io/registry/about)
is upstream metadata for downstream consumers; it does not supply a universal
agent recommendation score. Preserve attribution when reporting Glama's assessment.
