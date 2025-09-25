# Setting up Codex

This runbook explains how to connect your Bagel MCP server to Codex.

## ✅ Verify Bagel Is Running

But first, make sure the Bagel MCP server is already running in a separate terminal.

If not, follow the [⚡️ Quickstart](../../../README.md#️-quickstart) guide to start it.

You can check if it’s running by visiting [http://0.0.0.0:8000/sse](http://0.0.0.0:8000/sse)
in your browser. You should see output like:

```
event: endpoint
data: /messages/?session_id=d3daa0110c1041dead46bc6646dc4dc7
```

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

You should see output like:

```bash
codex-cli 0.31.0
```

Visit the [Codex CLI doc](https://developers.openai.com/codex/cli/) for more details.

## 🛠️ Install mcp-proxy

Because Bagel uses SSE transport and Codex currently supports only stdio transport,
we need an adapter: [mcp-proxy](https://github.com/sparfenyuk/mcp-proxy).

To install `mcp-proxy`, first [install](https://docs.astral.sh/uv/getting-started/installation/)
`uv` (skip this step if already installed):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then run:

```bash
uv tool install mcp-proxy
```

Verify the installation:

```bash
mcp-proxy --version
```

You should see output like:

```bash
mcp-proxy 0.8.2
```

## 🔗 Connect Bagel

To add the Bagel MCP server to Codex, open `~/.codex/config.toml` and add:

```toml
[mcp_servers.bagel]
command = "mcp-proxy"
args = ["http://localhost:8000/sse"]
env = { "API_KEY" = "<YOUR_OPENAI_API_KEY>" }
```

Now confirm the connection. Launch Codex and run:

```
/mcp list
```

You should see output like:

```bash
🔌  MCP Tools

  • Server: bagel
    • Command: mcp-proxy http://localhost:8000/sse
    • Tools: ...
```

For more details on connecting MCP servers to Codex, see the
[Codex on GitHub](https://github.com/openai/codex/blob/main/docs/config.md#mcp_servers).
