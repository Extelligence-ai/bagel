# Setting up Grok Build

This runbook explains how to connect your Bagel MCP server to Grok Build, the xAI coding agent CLI.

## ✅ Verify Bagel Is Running

But first, make sure the Bagel MCP server is already running in a separate terminal.

If not, follow the [⚡️ Quickstart](../../../README.md#️-quickstart) guide to start it.

You can check it is running:

```bash
curl -si http://localhost:8000/mcp -X POST \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}}' | head -1
```

You should see `HTTP/1.1 200 OK`.

## 🛠️ Install Grok Build

> [!NOTE]
> Grok Build requires an xAI account with Grok Build access.

Install Grok Build by following xAI's instructions, then verify the installation:

```bash
grok --version
```

Visit the [Grok Build plugin announcement](https://x.ai/news/grok-plugin-marketplace) for more details.

## 🔗 Connect Bagel

The easiest path is the bundled plugin. Grok Build reads the same `plugin/`
directory as Claude Code and Codex, so install it straight from this repository:

```bash
grok plugin install Extelligence-ai/bagel#plugin
```

The plugin adds four skills (log triage, pipeline authoring, live sinks,
visualization export) and connects Grok to `http://localhost:8000/mcp`
automatically.

To connect manually instead, without the skills:

```bash
grok mcp add bagel http://localhost:8000/mcp -t http
```

If you changed `MCP_SERVER_PORT` (for example because another service holds
8000), use that port in the URL. The plugin always points at 8000, so use the
manual command in that case.

Now confirm the connection:

```bash
grok plugin list
grok mcp doctor
```

Then launch `grok` and ask it to summarize the metadata of `./data/sample/ros2/mcap`.

## 🎉 Congrats! You are all set.

Still having trouble? 🤦 It's not your fault. [File a ticket](https://github.com/Extelligence-ai/bagel/issues) and let us know.
