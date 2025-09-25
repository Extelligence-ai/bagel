<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/bagel_logo_dark_mode.png">
    <img src="./doc/assets/bagel_logo_light_mode.png" width="400">
  </picture>
</p>

<h1 align="center">
  <a href="https://github.com/shouhengyi/bagel/blob/stage/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue?style=flat-square">
  </a>
  <a>
    <img src="https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square">
  </a>
  <a href="https://github.com/Extelligence-ai/bagel/actions/workflows/publish.yaml">
    <img src="https://img.shields.io/github/actions/workflow/status/Extelligence-ai/bagel/publish.yaml?branch=stage&label=publish&style=flat-square">
  </a>
  <a href="https://discord.gg/QJDwuDGJsH">
    <img src="https://img.shields.io/discord/1392632504908906506?label=Discord&style=flat-square">
  </a>
</h1>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/hero_dark_mode.png">
    <img src="./doc/assets/hero_light_mode.png" width="100%">
  </picture>
</p>

Bagel lets you chat with your physical data — just like you do with ChatGPT. For example:

> What’s the highest temperature in my log?

### 🥯 Key Features

- **Ask in plain language**: No deep domain expertise needed.
- **Transparent calculations**: Deterministic SQL queries. No black-box LLM math.
- **Broad LLM support**: Claude Code, Gemini, Cursor, Codex, and more.
- **Dockerized environments**: No local dependencies required.
- **Extensible capabilities**: Bagel can learn [new tricks](#-teach-bagel-a-new-trick).
- **Wide format coverage**: Missing your data format? [Open a ticket](https://github.com/shouhengyi/bagel/issues).

### ✅ Supported Data Formats

| Industry     | Formats                    |
| ------------ | -------------------------- |
| **Robotics** | ROS1, ROS2                 |
| **Drones**   | PX4, ArduPilot, Betaflight |
| **IoT**      | Coming soon...             |

## 💬 What Can I Prompt?

You can ask Bagel almost anything. For example:

> What’s the correlation between current and voltage in the `/spot/status/battery_states` topic?

> I think the IMU overheated at the end. Can you check the `HEAT` topic to confirm?

> Can you help me tune the PID of my drone?

Time to put Bagel to the test: can it catch a drone doing barrel rolls? Spoiler: 🎉 It totally can.

<p align="center">
  <picture>
    <img src="./doc/assets/drone_rolls.gif" width="80%">
  </picture>
</p>

## 💡 How Bagel Works

When you ask a question, Bagel analyzes your data source’s **metadata** and **topics** to
build a high-level understanding. Based on your prompt, it identifies the most relevant topics
and **interprets their meaning and structure**.

Bagel then processes this data in a local cache. It writes the relevant topic messages to an
**Apache Arrow file** and uses **DuckDB** to generate and execute queries against it.
This process is repeated as needed, running new queries until Bagel finds the best possible
answer to your question.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/llm_math_dark_mode.png">
    <img src="./doc/assets/llm_math_light_mode.png" width="80%">
  </picture>
</p>

LLMs excel at language but struggle with math. Bagel overcomes this by generating **deterministic**
DuckDB SQL queries. These queries are displayed for you to **audit**, and you can guide Bagel to
correct any errors.

## ⚡️ Quickstart

#### 📋 Prerequisites

Install [Docker Desktop](https://docs.docker.com/get-started/get-docker/), then clone this repo:

```bash
git clone https://github.com/Extelligence-ai/bagel.git; cd bagel
```

#### 🐳 Run with Docker

Open [compose.yaml](./compose.yaml) and pick a service (e.g., `ros2-kilted`).

```bash
docker compose run --service-ports ros2-kilted
```

> [!TIP]
> To give Bagel access to your local files, edit `compose.yaml` before starting Docker:
> uncomment and update the `volumes` section under your chosen service.

Your terminal should show output similar to:

```
INFO:     Started server process [86]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

#### 🔗 Connect to an LLM

Once the Bagel MCP server is running, connect it to your preferred LLM.
Bagel should work with any MCP-enabled LLM, though some setup may be required.
We’ve tested several LLMs—see the runbooks below for setup instructions.

<details>
  <summary>📚 Runbooks for tested LLMs</summary>

- [Claude Code](./doc/runbooks/setup/claude_code.md)
- [Gemini CLI](./doc/runbooks/setup/gemini_cli.md)
- [Codex](./doc/runbooks/setup/codex.md)
- [Cursor](./doc/runbooks/setup/cursor.md)
- [Copilot](./doc/runbooks/setup/copilot.md)

</details>

Can't find your favorite LLM? [Open a ticket](https://github.com/shouhengyi/bagel/issues),
and we’ll create a runbook for it.

#### 🚀 Launch and Prompt

Now you're ready to launch your LLM and begin prompting. For example:

> Summarize the metadata of the ROS2 bag "/home/ubuntu/runtime/data/sample/ros2/mcap".

## 🐶 Teach Bagel a New Trick

## 📚 Tutorials

## 🫶 Contributing

## 📄 License
