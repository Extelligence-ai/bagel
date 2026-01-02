# Setting up Claude Code

This runbook explains how to connect your Bagel MCP server to Claude Code.

## ✅ Verify Bagel Is Running

But first, make sure the Bagel MCP server is already running in a separate terminal.

If not, follow the [⚡️ Quickstart](../../../README.md#️-quickstart) guide to start it.

You can check if it’s running by visiting [http://0.0.0.0:8000/sse](http://0.0.0.0:8000/sse)
in your browser. You should see output like:

```
event: endpoint
data: /messages/?session_id=d3daa0110c1041dead46bc6646dc4dc7
```

## 🛠️ Install Claude Code

> [!NOTE]
> Claude Code requires a paid subscription from Anthropic.

Install Claude Code:

```bash
npm install -g @anthropic-ai/claude-code
```

Verify the installation:

```bash
claude --version
```

You should see output like:

```bash
1.0.123 (Claude Code)
```

Visit [Claude Code Quickstart](https://docs.claude.com/en/docs/claude-code/quickstart)
for more details.

## 🔗 Connect Bagel

Add the Bagel MCP server to Claude Code:

```bash
claude mcp add --transport sse bagel http://0.0.0.0:8000/sse
```

Confirm the connection:

```bash
claude mcp get bagel
```

You should see output like:

```bash
bagel:
  Scope: Local config (private to you in this project)
  Status: ✓ Connected
  Type: sse
  URL: http://0.0.0.0:8000/sse
```

For more details on connecting MCP servers to Claude Code, see the
[Claude Code docs](https://docs.claude.com/en/docs/claude-code/mcp).

### 🪄 Configure Claude Code

Bagel MCP tools may generate large outputs. If Claude Code reports token limit errors,
increase the limit with:

```bash
export MAX_MCP_OUTPUT_TOKENS=250000
```

For more details on setting token size, see the
[Claude Code docs](https://docs.claude.com/en/docs/claude-code/mcp#mcp-output-limits-and-warnings).

## 🎉 Congrats! You are all set.

Still having trouble? 🤦 It’s not your fault. [File a ticket](https://github.com/Extelligence-ai/bagel/issues) and let us know.
