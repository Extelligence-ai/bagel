# Smithery local connector

Source for <https://smithery.ai/servers/extelligence-ai/bagel>. The MCPB bridges
stdio to an already running local Bagel Docker service. See the bundled
[user instructions](bundle/README.md) for prerequisites, privacy, and costs.
It never hosts Bagel or requires a Matcha account.

## Build and validate

Requires Node 22.12+ and npm. From this directory:

```sh
npm ci --omit=dev --prefix bundle
npx --yes @anthropic-ai/mcpb@2.1.2 validate bundle/manifest.json
npx --yes @anthropic-ai/mcpb@2.1.2 pack bundle bagel-local.mcpb
```

Only `bundle/` is packaged. Publisher code, credentials, and the full tools
catalog remain outside the archive. Do not put secrets into that directory.
The locked runtime dependency is `mcp-remote` 0.14.3. Dependency licenses ship
with the bundle. Bundle versions are independent of Bagel image versions.

Start Bagel using the repository quickstart, then use an MCP stdio client with
`node bundle/server.mjs 8000`. Verify initialization and `tools/list` before
publishing. The port must be an integer in 1–65535; the host is always loopback.

## Publish deliberately

The existing 0.1.1 release was tested against local Bagel on October 6, 2026;
initialization and all 26 tool schemas were verified. This directory preserves
that release's source and tools catalog. The publishing configuration schema
also enforces the bridge's integer port range before startup. Do not republish
the existing release unnecessarily.

For a future release, update the bundle version, refresh `tools.json` from
the tested server's `tools/list` response, and regenerate `manifest.json`'s
`tools` entries using only each tool's `name` and `description`. The repository's
`scripts/export_tool_catalog.py` can export schemas from a configured Bagel
environment. Review the catalog for consistency with the server being tested.

Build and smoke-test first. Set `SMITHERY_API_KEY` through your normal secret
handling, using a publisher key with access to `extelligence-ai/bagel`, then:

```sh
node publish.mjs bagel-local.mcpb
```

The script uses Smithery's multipart release API with `type: stdio` and full
tool input schemas. Smithery CLI 1.2.0 copied the MCPB's summary-only `tools`
entries into the server card, which the API rejected because `inputSchema`
was missing. Keeping the schemas separate satisfies both MCPB validation and
the release API without adding invalid fields to the MCPB manifest.

Check the returned release reaches `SUCCESS`, inspect the public listing,
and compare the downloaded bundle to the tested file. Publishing is manual;
installing dependencies or running checks never publishes a release.
