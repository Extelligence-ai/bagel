# Setting up Codex

This runbook explains how to connect your Bagel MCP server to Codex.

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

## 🛠️ Install Codex

> [!NOTE]
> Codex requires a paid subscription from OpenAI.

Install Codex:

```bash
npm install -g @openai/codex
```

Verify the installation:

```bash
codex --version
```

Visit the [Codex CLI doc](https://developers.openai.com/codex/cli/) for more details.

## 🔗 Connect Bagel

Codex speaks MCP's streamable HTTP transport natively, and Bagel serves it at
`/mcp` by default: no proxy or adapter needed. The easiest path is the bundled
plugin: run Codex inside this repository and install `bagel` from `/plugins`
(the repo carries the marketplace entry).

To connect manually instead, open `~/.codex/config.toml` and add:

```toml
[mcp_servers.bagel]
url = "http://localhost:8000/mcp"
```

If you changed `MCP_SERVER_PORT` (for example because another service holds
8000), use that port in the URL.

Now confirm the connection. Launch Codex and run:

```
/mcp list
```

You should see the `bagel` server with its tools.

For more details on connecting MCP servers to Codex, see the
[Codex on GitHub](https://github.com/openai/codex/blob/main/docs/config.md#mcp_servers).

## 🎉 Congrats! You are all set.

Still having trouble? 🤦 It's not your fault. [File a ticket](https://github.com/Extelligence-ai/bagel/issues) and let us know.
