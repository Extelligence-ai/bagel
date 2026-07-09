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
- **Event-driven data reduction**: "Keep 10s around every hard brake, drop the rest" —
  previewed before a byte is written, live or in [batch](./doc/runbooks/data_reduction.md).
- **Broad LLM support**: Claude Code, Gemini, Cursor, Codex, and more.
- **Dockerized environments**: No local dependencies required.
- **Extensible capabilities**: Bagel can learn [new tricks](#-teach-bagel-a-new-trick).
- **Wide format coverage**: Missing your data format? [Open a ticket](https://github.com/Extelligence-ai/bagel/issues).

### ✅ Supported Data Formats

| Industry     | Formats                        |
| ------------ | ------------------------------ |
| **Robotics** | ROS1, ROS2, MCAP (any profile), ROS text logs (`~/.ros/log`) |
| **Drones**   | PX4, ArduPilot, Betaflight     |
| **IoT**      | MQTT (live, Sparkplug B), PostgreSQL / TimescaleDB, InfluxDB 3 |

## 🆚 Bagel vs. the Tools You Already Use

You already have `ros2 *`, PlotJuggler, and grep. Bagel doesn't replace them — it
answers the questions they make you work for, then hands off to them:

| You do this today | Ask Bagel instead |
| --- | --- |
| `ros2 bag info` for metadata | *"Summarize this bag"* — same prompt works on PX4, ArduPilot, MCAP, MQTT, Postgres |
| `ros2 topic echo /imu` and eyeball raw values | *"What's the peak z-deceleration in /imu? Running average over 5 s?"* — real SQL underneath: peaks, running averages, percentiles, cross-topic correlations |
| Scrub PlotJuggler timelines hunting for the event | *"Find every deceleration under −10 m/s² and cut ±30 s snippets"* — then open the result in PlotJuggler with a [pre-framed layout](./doc/runbooks/plotjuggler.md) |
| `rqt_console`, or grep `~/.ros/log` | *"Read the ERRORs from ~/.ros/log and tell me what went wrong"* — tracebacks included, [no bag needed](./doc/runbooks/ros_text_logs.md) |
| Echo two topics in two terminals, correlate in a spreadsheet | *"What's the correlation between current and voltage?"* — topics live in one SQL relation, so joins and `corr()` are one question |
| `ros2 bag record -a` and babysit the disk | A [standing edge pipeline](./doc/runbooks/data_reduction.md): record continuously, keep only event windows, drop the rest |
| A bash loop over 200 bags | *"Run this pipeline on every bag in the folder"* — [one pipeline, whole fleet](./doc/runbooks/data_reduction.md), with a combined report |
| `scp`/`aws s3 sync` scripts to ship data off the robot | Upload to S3, GCS, or Azure as a pipeline step, checksum-skipping files already there |
| A different viewer per format: FlightPlot for PX4, MAVExplorer for ArduPilot, Blackbox Explorer for Betaflight | The same conversation for all of them — and ROS, MCAP, MQTT, Postgres, InfluxDB |
| Write a one-off pandas script per question | Ask the question; Bagel writes and runs the query |

One sentence of plain language, one answer — instead of a pipeline of commands and
a script you'll delete tomorrow. Here it is reducing a bag around detected events:

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/nl_reduction_dark_mode.gif">
    <img src="./doc/assets/nl_reduction_light_mode.gif" width="80%">
  </picture>
</p>

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
| `iot`             | IoT / MQTT (live)       |

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

#### 🔒 Prefer fully offline?

Swap step 2 for a local model — your data *and* your LLM stay on the machine:

```bash
brew install ollama && ollama serve &                                  # or ollama.com
ollama pull qwen3:8b
uvx ollmcp --mcp-server-url http://localhost:8000/sse --model qwen3:8b
```

Model picks, expectations, and troubleshooting: [Local LLMs guide](./doc/runbooks/local_llm.md).

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

## 📚 Guides

- [Event-driven data reduction](./doc/runbooks/data_reduction.md) — detect events, keep
  windows around them (snippets or one reduced bag), batch across fleets, upload to the cloud
- [Live ROS2 robots over rosbridge](./doc/tutorials/live_ros2_bridge.md) — a step-by-step tutorial
- [ROS text logs](./doc/runbooks/ros_text_logs.md) — inspect `~/.ros/log` errors and warnings without opening a bag
- [MQTT](./doc/runbooks/iot_mqtt.md) — live IoT topics, Sparkplug B, edge recording
- [PostgreSQL / TimescaleDB](./doc/runbooks/iot_postgres.md) — every table is a topic
- [InfluxDB 3](./doc/runbooks/iot_influxdb.md) — every measurement is a topic
- [PlotJuggler](./doc/runbooks/plotjuggler.md) — open Bagel's MCAP outputs directly; flattened CSV/Parquet exports
- [Local LLMs](./doc/runbooks/local_llm.md) — fully offline with Ollama: your data and your model never leave the machine

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
