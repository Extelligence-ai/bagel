<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./doc/assets/bagel_logo_dark_mode.png">
    <img src="./doc/assets/bagel_logo_light_mode.png" width="400">
  </picture>
</p>

<h1 align="center">
  <a href="https://github.com/shouhengyi/bagel/blob/stage/LICENSE">
    <img alt="Apache" src="https://img.shields.io/badge/license-Apache-blue.svg">
  </a>
  <a href="https://discord.gg/QJDwuDGJsH">
    <img alt="Rerun Discord" src="https://img.shields.io/discord/1392632504908906506?label=Discord">
  </a>
</h1>

---

Bagel lets you chat with your robotics data.

For example, you can ask Bagel to check a PX4 drone's IMU data for hard landings:

<p align="center">
  <picture>
    <img src="./doc/assets/prompt_hard_landing.gif" width="90%">
  </picture>
</p>

Bagel can also propose fixes for detected errors in your robot:

<p align="center">
  <picture>
    <img src="./doc/assets/prompt_find_errors.gif" width="90%">
  </picture>
</p>

Bagel works with a wide range of common robotics and sensor log formats out of the box. Don't see your format? [Open a ticket](https://github.com/shouhengyi/bagel/issues).

| Format                         |
| ------------------------------ |
| ✅ **ROS 2** (`.mcap`, `.db3`) |
| ✅ **ROS 1** (`.bag`)          |
| ✅ **PX4** (`.ulg`)            |

Don't forget to join our [Discord server](https://discord.gg/QJDwuDGJsH)!

## Quickstart

We are using ROS2 Kilted and Claude Code as example.

```sh
source /opt/ros/kilted/setup.sh  # Source ROS2 dependencies
uv sync --group ros2             # Install PyPI packages
uv run main.py up mcp            # Start Bagel MCP server
```

Open another terminal and run:

```sh
# Add Bagel MCP server to Claude Code
claude mcp add --transport sse bagel http://localhost:8000/sse

# Launch Claude Code
claude

# Happy prompting
> Summarize the metadata of robolog "./doc/tutorials/data/ros2".
```

The Bagel MCP server is **not exclusively tied to Claude**. You're free to integrate your
preferred LLMs with Bagel.

### Tutorials

- [Claude Code Tutorial with a PX4 ULog](./doc/tutorials/mcp/0_claude_code_px4.ipynb)
- [Gemini CLI Tutorial with a ROS2 Bag](./doc/tutorials/mcp/1_gemini_cli_ros2.ipynb)
- [Cursor Tutorial with a PX4 ULog](./doc/tutorials/mcp/2_cursor_px4.ipynb)
- [Build a Data Pipeline from a PX4 ULog](./doc/tutorials/pipelines/0_basics.ipynb)
- [Read Topic Messages from a ROS2 Bag](./doc/tutorials/readers/1_read_by_topic.ipynb)

### Running in Docker 🐳

To run Bagel without installing local dependencies like ROS, you can use our provided Docker images. Make sure you have [Docker Desktop](https://docs.docker.com/desktop/) installed. This example uses ROS 2 Kilted.

#### Mount Your Data

First, give the container access to your robolog files. Open the [compose.yaml](./compose.yaml) file and find the service you want to use (e.g., ros2-kilted). Edit the volumes section to link your local data folder to the container's data folder.

```yaml
services:
  ros2-kilted:
    ...
    # volumes:                                     <-- ✅ Uncomment
    #   - <path-to-local-data>:/home/ubuntu/data   <-- ✅ Uncomment & Replace
```

Your local robolog files will be accessible inside the container at `/home/ubuntu/data`.

#### Launch the MCP Server

Build and start the Bagel MCP server in a container with this command.

```sh
docker compose run --build --service-ports ros2-kilted uv run main.py up mcp
```

#### Roadmap

If there's something you have feedback on, or something you'd like to see let us know!

These are grouped into " Versions" for us to keep track, features will be released regularly. We'll update this readme to indicate which features have shipped. 

V1 
-CV Module - Computer vision module to let you search for and detect anomalies in video. 
-Ardupilot Support 
-Betaflight Support  
-Open AI Support
-Llama support
V1.5 
-Easy Model Integration
-Topic Frequency status 
-MCP Troubleshooting toolkit 

V2
-Foxglove Integration
-Rerun Integration 
-CLI Support 
-PyPi support 
-VSCode Support
