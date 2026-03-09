<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/bagel_logo_dark_mode.png">
    <img src="./doc/assets/bagel_logo_light_mode.png" width="400">
  </picture>
</p>

<h1 align="center">
  <a href="https://github.com/Extelligence-ai/bagel/blob/stage/LICENSE">
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

Unlike cat videos, most data are trapped in the physical world. Bagel unlocks them and
**lets you chat with your physical data**—just like you do with ChatGPT. For example:

> Is my IMU sensor overheating?

Can’t wait to try it out? 👉 Check out the [Quickstart](#️-quickstart).

### 🥯 Key Features

- **Ask in plain language**: No deep domain expertise needed.
- **Transparent calculations**: Deterministic SQL queries. No black-box LLM math.
- **Broad LLM support**: Claude Code, Gemini, Cursor, Codex, and more.
- **Dockerized environments**: No local dependencies required.
- **Extensible capabilities**: Bagel can learn [new tricks](#-teach-bagel-a-new-trick).
- **Wide format coverage**: Missing your data format? [Open a ticket](https://github.com/Extelligence-ai/bagel/issues).

### ✅ Supported Data Formats

| Industry     | Formats                    |
| ------------ | -------------------------- |
| **Robotics** | ROS1, ROS2                 |
| **Drones**   | PX4, ArduPilot, Betaflight |
| **IoT**      | Coming soon...             |

## 💬 What Can I Prompt?

You can ask Bagel almost anything. For example:

> What’s the correlation between current and voltage in the `/spot/status/battery_states` topic?

> I think the robot hit a pothole. Can you check for sudden deceleration on the z-axis to confirm?

> Can you help me tune the PID of my drone?

Time to put Bagel to the test: can it catch a drone doing barrel rolls? Spoiler: 🎉 It totally can.

<p align="center">
  <picture>
    <img src="./doc/assets/drone_rolls.gif" width="80%">
  </picture>
</p>

## 💡 How Bagel Works

When you ask a question, Bagel analyzes your data source’s **metadata** and **topics** to
build a high-level understanding.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/high_level_dark_mode.png">
    <img src="./doc/assets/high_level_light_mode.png" width="80%">
  </picture>
</p>

Based on your prompt, if further inspection is needed, Bagel identifies the most relevant topics
and **interprets their meaning and structure**. Bagel then writes the relevant topic messages
to an **Apache Arrow file** and uses **DuckDB** to generate and execute queries against it.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/llm_math_dark_mode.png">
    <img src="./doc/assets/llm_math_light_mode.png" width="80%">
  </picture>
</p>

This process is repeated as needed, running new queries until Bagel finds the best answer
to your question.

LLMs excel at language but struggle with math. Bagel overcomes this by generating **deterministic**
DuckDB SQL queries. These queries are displayed for you to **audit**, and you can guide Bagel to
correct any errors.

## ⚡️ Quickstart

Three commands. That’s it.

> [!TIP]
> **Already have Claude Code?** Just paste the link to this repo and tell Claude
> what environment you want:
>
> > Set up https://github.com/Extelligence-ai/bagel for ROS2 Kilted.
>
> Claude will clone the repo, start Docker, and wire up the MCP connection for you.

#### 📋 Prerequisites

Install [Docker Desktop](https://docs.docker.com/get-started/get-docker/) and
[Claude Code](https://docs.claude.com/en/docs/claude-code/quickstart) (or another MCP-enabled LLM).

#### 1. Clone and start Bagel

```bash
git clone https://github.com/Extelligence-ai/bagel.git && cd bagel
docker compose run --service-ports ros2-kilted
```

Pick the service that matches your environment:

| Service           | Use case                |
| ----------------- | ----------------------- |
| `ros2-kilted`     | ROS2 Kilted (latest)    |
| `ros2-jazzy`      | ROS2 Jazzy              |
| `ros2-iron`       | ROS2 Iron               |
| `ros2-humble`     | ROS2 Humble             |
| `ros1-noetic`     | ROS1 Noetic             |
| `ros1-noetic-cv`  | ROS1 Noetic + CV        |
| `px4`             | PX4 flight logs         |
| `ardupilot`       | ArduPilot flight logs   |
| `betaflight`      | Betaflight flight logs  |

> [!TIP]
> To give Bagel access to your local files, edit `compose.yaml` before starting Docker:
> uncomment and update the `volumes` section under your chosen service.

Wait for this output:

```
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
```

#### 2. Connect Claude Code

In a new terminal:

```bash
claude mcp add --transport sse bagel http://0.0.0.0:8000/sse
```

#### 3. Prompt

```bash
claude
```

> Summarize the metadata of the ROS2 bag "./data/sample/ros2/mcap".

That’s it — you’re chatting with your data.

<details>
  <summary>📚 Using a different LLM?</summary>

Bagel works with any MCP-enabled LLM. Setup runbooks for tested alternatives:

- [Claude Code](./doc/runbooks/setup/claude_code.md) (detailed guide)
- [Gemini CLI](./doc/runbooks/setup/gemini_cli.md)
- [Codex](./doc/runbooks/setup/codex.md)
- [Cursor](./doc/runbooks/setup/cursor.md)
- [Copilot](./doc/runbooks/setup/copilot.md)

Can’t find your LLM? [Open a ticket](https://github.com/Extelligence-ai/bagel/issues).

</details>

## 🐶 Teach Bagel a New Trick

Bagel learns new capabilities through [POML](https://microsoft.github.io/poml/latest/)
files—a structured set of instructions that describe a “trick,”
such as [computing latency statistics](./src/agent/diagnose/latency.poml).

#### ✍️ Create a .poml file

For example, let’s define `./src/agent/examples/woof.poml`.

```poml
<poml>
    <task>
        Count the topics in the data source.
        If the count is odd, say "woof", else say "meow".
    </task>

    <output-format>
        Return the sound, the topic count, and a few cute emojis. Nothing else.
    </output-format>
</poml>
```

#### 🗣️ Use the capability

Prompt Bagel:

> Run the POML capability "./src/agent/examples/woof.poml" on the ROS2 bag "./data/sample/ros2/mcap".

Result:

```
meow 🐱 4 topics 🐱💤🎯
```

## 📦 Integrations

- [Cloudini](./doc/runbooks/cloudini.md) — Decode cloudini-compressed pointcloud data in pipelines

## 🫶 Contributing

We’d love your help! The easiest way to support the project is by giving it a ⭐ on GitHub.

Other great ways to contribute:

- Request new features
- Report bugs
- Improve documentation
- Add new capabilities

Before contributing, please review the [guidelines](./CONTRIBUTING.md).

Join the conversation in our [Discord server](https://discord.com/invite/QJDwuDGJsH) —
we hang out there regularly.

## 📄 License

Bagel is open source under the [Apache License 2.0](./LICENSE).
