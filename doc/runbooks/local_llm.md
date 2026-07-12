# Chat with your robot data using a local LLM

Everything in this walkthrough runs on your machine: Bagel, the model, and the
conversation. No tokens, no API keys, no data leaving the network -- the
strongest version of the privacy story for teams whose bags can't touch a cloud.

The stack is three pieces:

1. **Bagel** -- the MCP server (you already have this)
2. **[Ollama](https://ollama.com)** -- serves the local model
3. **[ollmcp](https://github.com/jonigl/mcp-client-for-ollama)** -- a terminal
   MCP client that speaks to both

## 1. Start Bagel

As usual, e.g.:

```bash
docker compose up ros2-jazzy
```

The MCP server listens on `http://localhost:8000/sse`.

## 2. Start Ollama and pull a tool-calling model

Natively (recommended on macOS -- it uses the GPU):

```bash
brew install ollama        # or the installer from ollama.com
ollama serve &
ollama pull qwen3:8b
```

Or via the bundled compose profile (Linux hosts):

```bash
docker compose --profile local-llm up -d ollama
docker compose exec ollama ollama pull qwen3:8b
```

The model must support **tool calling** -- that's what lets it invoke Bagel's
tools instead of just talking about them. Good picks, smallest first:

| Model | RAM | Notes |
| --- | --- | --- |
| `qwen3:4b` | ~8 GB | verified with this walkthrough |
| `qwen3:8b` | ~16 GB | better SQL; good default |
| `llama3.1:8b` | ~16 GB | solid alternative |

## 3. Connect them

```bash
uvx ollmcp --mcp-server-url http://localhost:8000/sse --model qwen3:8b
```

That's the whole setup. `ollmcp` discovers Bagel's tools and hands them to the
model.

## 4. Chat

Try the bundled sample data:

> How many rows are in ./data/sample/pyarrow/csv?

> Read all ERROR messages from ./data/sample/ros/log and summarize what went wrong.

Then point it at your own bags, MCAP files, or `~/.ros/log`.

## Expectations, honestly

A 4-8B local model is not a frontier model. It handles tool selection and
simple SQL well, but complex multi-step pipelines (event-windowed reduction,
multi-topic joins) benefit from a bigger model -- `qwen3:32b` if you have the
RAM, or a hosted model when the data sensitivity allows it. Start small, scale
up when you see the model struggle.

## Troubleshooting

- **Model never calls tools** -- it probably doesn't support tool calling.
  Stick to the table above or check the
  [Ollama models page](https://ollama.com/search?c=tools) for the `tools` tag.
- **`connection refused` from ollmcp** -- Bagel isn't up or the port differs;
  check `MCP_SERVER_PORT` in `.env`.
- **Ollama-in-Docker is slow on a Mac** -- expected; Docker Desktop has no GPU
  passthrough. Run Ollama natively on macOS.
- **Painfully slow on an Apple Silicon Mac** -- check `file $(which ollama)`.
  If it says `x86_64`, you have the Intel Homebrew build running under Rosetta:
  CPU-only, no Metal. Install the native arm64 build from
  [ollama.com](https://ollama.com/download) instead.
