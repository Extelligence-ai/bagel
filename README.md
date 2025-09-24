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
    <img src="./doc/assets/hero.png" width="90%">
  </picture>
</p>

Bagel lets you chat with your physical data — just like you do with ChatGPT. For example:

> What’s the highest temperature in my log?

### 🪄 Key Features

- **Ask in plain language**: No deep domain expertise needed.
- **Transparent calculations**: Deterministic SQL queries. No black-box LLM math.
- **Broad LLM support**: Claude Code, Gemini, Cursor, Codex, and more.
- **Dockerized environments**: No local dependencies required.
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

<p align="center">
  <picture>
    <img src="./doc/assets/llm_math.png" width="80%">
  </picture>
</p>

Bagel then processes this data in a local cache. It writes the relevant topic messages to an
**Apache Arrow file** and uses **DuckDB** to generate and execute queries against it.
This process is repeated as needed, running new queries until Bagel finds the best possible
answer to your question.

LLMs excel at language but struggle with math. Bagel overcomes this by generating **deterministic**
DuckDB SQL queries. These queries are displayed for you to **audit**, and you can guide Bagel to
correct any errors.

## ⚙️ Installation

## ⚡️ Quickstart

## 💻 Development
