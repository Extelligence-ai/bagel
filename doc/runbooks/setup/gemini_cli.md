# Setting up Gemini CLI

This runbook explains how to connect your Bagel MCP server to Gemini CLI.

## ✅ Verify Bagel Is Running

But first, make sure the Bagel MCP server is already running in a separate terminal.

If not, follow the [⚡️ Quickstart](../../../README.md#️-quickstart) guide to start it.

You can check if it’s running by visiting [http://0.0.0.0:8000/sse](http://0.0.0.0:8000/sse)
in your browser. You should see output like:

```
event: endpoint
data: /messages/?session_id=d3daa0110c1041dead46bc6646dc4dc7
```

## 🛠️ Install Gemini CLI

Install Gemini CLI:

```bash
npm install -g @google/gemini-cli
```

Verify the installation:

```bash
gemini --version
```

You should see output like:

```bash
0.5.5
```

Visit [Gemini CLI on GitHub](https://github.com/google-gemini/gemini-cli) for more details.

## 🔗 Connect Bagel

Add the Bagel MCP server to Gemini CLI:

```bash
gemini mcp add -t sse bagel http://0.0.0.0:8000/sse
```

Confirm the connection:

```bash
gemini mcp list
```

You should see output like:

```bash
Configured MCP servers:

✓ bagel: http://0.0.0.0:8000/sse (sse) - Connected
```

For more details on connecting MCP servers to Gemini CLI, see the
[Gemini CLI docs](https://github.com/google-gemini/gemini-cli/blob/main/docs/tools/mcp-server.md).

## 🎉 Congrats! You are all set.

Still having trouble? 🤦 It’s not your fault. [File a ticket](https://github.com/Extelligence-ai/bagel/issues) and let us know.
