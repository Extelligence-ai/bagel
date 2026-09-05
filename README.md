<p align="center">
  <img src="./doc/assets/bagel_logo_light_mode.png" width="560">
</p>

<h1 align="center">
  <a href="https://github.com/Extelligence-ai/bagel/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue?style=flat-square">
  </a>
  <a>
    <img src="https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square">
  </a>
  <a href="https://github.com/Extelligence-ai/bagel/actions/workflows/test.yaml">
    <img src="https://img.shields.io/github/actions/workflow/status/Extelligence-ai/bagel/test.yaml?event=pull_request&label=tests&style=flat-square">
  </a>
  <a href="https://github.com/Extelligence-ai/bagel/actions/workflows/publish.yaml">
    <img src="https://img.shields.io/github/actions/workflow/status/Extelligence-ai/bagel/publish.yaml?branch=main&label=publish&style=flat-square">
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

Bagel lets you ask questions about robotics, drone, and IoT data in plain English.
Every calculation over your message data is DuckDB SQL, not model guesswork, and
Bagel shows you the query so you can audit it.

> Is my IMU sensor overheating?

Bagel also has an intelligent edge data reduction pipeline: describe an event and
Bagel runs the detection on the robot, keeping the windows that matter and dropping
the rest. An MCP server puts all of it in your LLM's hands: Claude Code, Gemini,
Cursor, or a fully local model.

Bagel was the first MCP server to ship a real analysis toolkit for robotics data,
and it keeps the LLM where it belongs: in front of your logs, never in your robot's
control loop.

### 🥯 Key Features

- **Ask in plain language**: No deep domain expertise needed.
- **Transparent calculations**: Deterministic SQL queries. No black-box LLM math.
- **Natural-language pipelines**: "Keep 10s around every hard brake, drop the rest":
  one sentence becomes an auditable [pipeline](./doc/runbooks/pipelines.md): previewed
  before a byte is written, then run once, across a fleet, or standing at the edge.
- **Broad LLM support**: Claude Code, Gemini, Cursor, Codex, and more.
- **Dockerized environments**: No local dependencies required.
- **Extensible capabilities**: Bagel can learn [new tricks](#-teach-bagel-a-new-trick).
- **Wide format coverage**: Missing your data format? [Open a ticket](https://github.com/Extelligence-ai/bagel/issues).

## 🥯 Try it in 60 seconds

No MCP client, no LLM, no config: run the same deterministic checks against a
bundled sample log and get a robot-health report card straight to your
terminal.

```bash
docker run -it --rm ghcr.io/extelligence-ai/bagel/px4:latest demo
```

```
sample.ulg - 41.5s, 2018 messages, 77 topics

Power      ⚠️  min 21.07V, largest drop 2.37V at ~t=+4.8s, end 23.45V (battery_status_0)
IMU        ✅  accel_z stddev 1.6x the log baseline at ~t=+36.8s (sensor_combined_0)
GPS        —  skipped: no GPS topic
Data gaps  ✅  no gap > 1.05x median interval (checked battery_status_0, sensor_combined_0)
...
```

The ROS2 images (`ros2-kilted`, `ros2-jazzy`, `ros2-iron`, `ros2-humble`) run
`demo` the same way, against a lighter bundled sample (`px4` is the one that
ships with a flight log rich enough to show every check). Point it at your
own log with `demo /path/to/log` (mount it with `-v` first), or keep reading
for the full MCP setup below.

## ⚡️ Quickstart

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

> [!TIP]
> Port 8000 already in use? Set `MCP_SERVER_PORT` to something else, for example
> `MCP_SERVER_PORT=8100 docker compose run --service-ports ros2-kilted`, and use
> that port in step 2.

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
claude mcp add --transport sse bagel http://localhost:8000/sse
```

> [!NOTE]
> The MCP endpoint is bound to `localhost` only (not exposed to the LAN) for security.
> To share it with other machines, drop the `127.0.0.1` prefix in `compose.yaml` and
> put an authenticated proxy in front: see [SECURITY.md](./SECURITY.md).

#### 3. Prompt

```bash
claude
```

> Summarize the metadata of the ROS2 bag "./data/sample/ros2/mcap".

That’s it: you’re chatting with your data.

#### 🔒 Prefer fully offline?

Swap step 2 for a local model: your data *and* your LLM stay on the machine:

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

## 🔌 Agent plugins (Claude Code and Codex)

Bagel ships an agent plugin: four skills that teach the agent when and how
to drive the server (log triage, pipeline authoring, live sinks, visualization
export) plus the MCP connection, wired automatically. The same `plugin/`
directory serves both Claude Code and OpenAI Codex.

```
/plugin marketplace add Extelligence-ai/bagel
/plugin install bagel@bagel
```

Codex and ChatGPT users: install bagel from the
[OpenAI Plugins Directory](https://chatgpt.com/plugins/plugins_6a8623a0fe288191833ee0ca3fa883e7)
(one click), or clone the repo and add it as a plugin marketplace (the repo
carries `.agents/plugins/marketplace.json`). Directory installs bundle the
skills only, so also connect the server once in `~/.codex/config.toml`:

```toml
[mcp_servers.bagel]
url = "http://localhost:8000/mcp"
```

Repo-marketplace and Claude Code installs wire this connection automatically.

Then start the container for your data format (see Quickstart): the plugin
connects to `http://localhost:8000/mcp` by default. Any other MCP client can
discover the same workflows server-side via the `list_agent_capabilities` tool.

## Keep what matters, drop the rest

A robot records more data than you can afford to move. Bagel turns a question into
a detector, runs it where the data is recorded, and ships only the windows around
real events.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/edge_reduce_dark_mode.svg">
    <img src="./doc/assets/edge_reduce_light_mode.svg" width="80%">
  </picture>
</p>

Here it is in one conversation:

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/nl_reduction_dark_mode.gif">
    <img src="./doc/assets/nl_reduction_light_mode.gif" width="80%">
  </picture>
</p>

The session above: a 20-minute (1,200 s) recording and the prompt *"keep 10 seconds
before and after every deceleration harder than −10 m/s²"*. The preview detects
7 events, merges them into 4 windows, and keeps 92 s of the 1,200 (7.6%); the run
writes a 2.1 GB bag down to 161 MB. These figures are illustrative demo output, not
a measured benchmark: the ratio is event-window duration over total duration, so it
depends entirely on your workload.

### ✅ Supported Data Formats

| Industry     | Formats                        |
| ------------ | ------------------------------ |
| **Robotics** | ROS1, ROS2, MCAP (any profile), [Copper](./doc/runbooks/copper.md) (via MCAP export), ROS text logs (`~/.ros/log`) |
| **Drones**   | PX4, ArduPilot, Betaflight     |
| **Automotive** | ASAM MDF4 (`.mf4`), CAN captures (`.blf`/`.asc` + DBC) · *beta* |
| **IoT**      | MQTT (live, Sparkplug B), PostgreSQL / TimescaleDB, InfluxDB 3 |
| **Hardware state** | [WaffleForm](./doc/runbooks/waffle.md) snapshots (`.waffleform.yaml`), auto-detected via waffle-iron · *beta* |

## 🆚 Bagel vs. the Tools You Already Use

You already have `ros2 *`, PlotJuggler, and grep. Bagel doesn't replace them: it
answers the questions they make you work for, then hands off to them:

| You do this today | Ask Bagel instead |
| --- | --- |
| `ros2 bag info` for metadata | *"Summarize this bag"*: same prompt works on PX4, ArduPilot, MCAP, MQTT, Postgres |
| `ros2 topic echo /imu` and eyeball raw values | *"What's the peak z-deceleration in /imu? Running average over 5 s?"* · real SQL underneath: peaks, running averages, percentiles, cross-topic correlations |
| Scrub PlotJuggler timelines hunting for the event | *"Find every deceleration under −10 m/s² and cut ±30 s snippets"*: then open the result in PlotJuggler with a [pre-framed layout](./doc/runbooks/plotjuggler.md) |
| `rqt_console`, or grep `~/.ros/log` | *"Read the ERRORs from ~/.ros/log and tell me what went wrong"*: tracebacks included, [no bag needed](./doc/runbooks/ros_text_logs.md) |
| Echo two topics in two terminals, correlate in a spreadsheet | *"What's the correlation between current and voltage?"*: topics live in one SQL relation, so joins and `corr()` are one question |
| `ros2 bag record -a` and babysit the disk | A [standing edge pipeline](./doc/runbooks/data_reduction.md): record continuously, keep only event windows, drop the rest |
| A bash loop over 200 bags | *"Run this pipeline on every bag in the folder"*: [one pipeline, whole fleet](./doc/runbooks/data_reduction.md), with a combined report |
| `scp`/`aws s3 sync` scripts to ship data off the robot | Upload to S3, GCS, or Azure as a pipeline step, checksum-skipping files already there |
| A different viewer per format: FlightPlot for PX4, MAVExplorer for ArduPilot, Blackbox Explorer for Betaflight | The same conversation for all of them, and ROS, MCAP, MQTT, Postgres, InfluxDB |
| Write a one-off pandas script per question | Ask the question; Bagel writes and runs the query |

One sentence of plain language, one answer, instead of a pipeline of commands and
a script you'll delete tomorrow.

## 💬 What Can I Prompt?

You can ask Bagel almost anything. For example:

> What’s the correlation between current and voltage in the `/spot/status/battery_states` topic?

> I think the robot hit a pothole. Can you check for sudden deceleration on the z-axis to confirm?

> Every time the drone decelerates harder than -10 m/s², keep 10 seconds before and after. Drop everything else.

> Did anything change on this robot since last week?

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

## 🐶 Teach Bagel a New Trick

Bagel learns new capabilities through [POML](https://microsoft.github.io/poml/latest/)
files: a structured set of instructions that describe a “trick,”
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

### Teach it your own tricks (no rebuild)

Bagel discovers your own capabilities from `~/.bagel/capabilities/`:

- **In conversation:** do a workflow once, then say *"save that as a
  capability called battery-triage"* — Claude calls `save_agent_capability`
  and it's reusable in any future session.
- **As a file:** drop a markdown file with your steps (or a
  [POML](https://github.com/microsoft/poml) file, if you want parameterized
  templates — see `src/agent/compose/pipeline.poml` for the house style)
  into `~/.bagel/capabilities/`.

Either way it shows up in `list_agent_capabilities` as `user/<name>` and runs
with `run_poml_capability` — from Claude Code, Claude Desktop, or any MCP
client. Teams: keep the directory in your own git repo and sync it to every
robot; it's just files. On Linux, run `mkdir -p ~/.bagel/capabilities` once
before starting the container so the mount is owned by you, not root.

## 📚 Guides

- [Natural-language pipelines](./doc/runbooks/pipelines.md) · the model: a cadence, gates,
  and tasks; preview → run → save → batch → standing at the edge
- [Event-driven data reduction](./doc/runbooks/data_reduction.md) · detect events, keep
  windows around them (snippets or one reduced bag), batch across fleets, upload to the cloud
- [Live ROS2 robots over rosbridge](./doc/tutorials/live_ros2_bridge.md) · a step-by-step tutorial
- [ROS text logs](./doc/runbooks/ros_text_logs.md) · inspect `~/.ros/log` errors and warnings without opening a bag
- [MQTT](./doc/runbooks/iot_mqtt.md) · live IoT topics, Sparkplug B, edge recording
- [PostgreSQL / TimescaleDB](./doc/runbooks/iot_postgres.md) · every table is a topic
- [InfluxDB 3](./doc/runbooks/iot_influxdb.md) · every measurement is a topic
- [Automotive MDF4 & CAN](./doc/runbooks/automotive_mdf.md) *(beta)* · channel groups and DBC messages are topics; units ride along
- [Local LLMs](./doc/runbooks/local_llm.md) · fully offline with Ollama: your data and your model never leave the machine

## 📦 Integrations

- [Rerun](./doc/runbooks/rerun.md) · "show me that event in Rerun": any time window as a ready-to-open recording
- [Lichtblick / Foxglove](./doc/runbooks/lichtblick.md) · event windows as MCAP + pre-framed layouts for either viewer
- [PlotJuggler](./doc/runbooks/plotjuggler.md) · open Bagel's MCAP outputs directly; one-sentence pre-framed sessions, flattened CSV/Parquet exports
- [Cloudini](./doc/runbooks/cloudini.md) · decode cloudini-compressed pointclouds, or compress a bag's PointCloud2 topics into CompressedPointCloud2
- [Slack](./doc/runbooks/pipelines.md) · pipelines post to your ops channel when they fire: "🚨 hard brake on {asset}"
- [LeRobot](./doc/runbooks/lerobot.md) *(beta)* · detected events become training episodes: a LeRobotDataset v3.0

## 🚧 Limitations

Rough edges we know about, so you don't find them the hard way:

- **Two formats are beta.** The automotive MDF4/CAN readers are verified against
  files we generate with the same libraries that read them (`asammdf`, `python-can`);
  real CANape/INCA/Vector-produced captures haven't crossed our test bench yet.
  LeRobot exports load-test clean with the real `lerobot` package, but no policy
  has been trained from a Bagel export yet.
- **Reduction ratios are workload-dependent, and unbenchmarked.** The ratio is
  event-window duration over total duration: quiet recordings reduce dramatically,
  eventful ones much less. The figures in this README are illustrative demo output,
  not a measured benchmark.
- **No authentication on the MCP endpoint.** By design it binds to localhost only;
  treat it like a database socket and see [SECURITY.md](./SECURITY.md) before
  sharing it beyond your machine.
- **Small local models struggle with multi-step pipelines.** A 4-8B model handles
  tool selection and simple SQL; event-windowed reduction and multi-topic joins
  want a bigger model. See the [Local LLMs guide](./doc/runbooks/local_llm.md).
- **Live-database end-to-end tests run outside CI.** The InfluxDB and Postgres
  suites' pure tests run in CI; their live end-to-end cases only execute against
  an instance you point them at. Everything else, including the ROS bag write
  paths, runs in CI.

## 🫶 Contributing

We’d love your help! The easiest way to support the project is by giving it a ⭐ on GitHub.

Other great ways to contribute:

- Request new features
- Report bugs
- Improve documentation
- Add new capabilities

Before contributing, please review the [guidelines](./CONTRIBUTING.md).

Join the conversation in our [Discord server](https://discord.com/invite/QJDwuDGJsH).
We hang out there regularly.

## 📄 License

Bagel is open source under the [Apache License 2.0](./LICENSE).
