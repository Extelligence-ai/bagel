# Setting up Grok Bot

This runbook explains how to run Bagel inside Grok Bot, xAI's agent app.

Grok Bot gives your account its own cloud computer, and it can run MCP servers
there from a start command. Bagel runs on that computer: you don't need to
install anything on your own machine, and you don't need a hosted server.

> [!NOTE]
> Bagel runs on Grok Bot's cloud computer, not on your laptop, so the data you
> analyze has to be there too. To keep robot data on your own machine, use
> [Grok Build](./grok_build.md) or another local client instead.

## 🔗 Connect Bagel

Paste this into Grok Bot:

> Add an MCP server named bagel that runs the command
> `uvx bagel-mcp --transport stdio` (install uv with `pip install uv` first if
> it's missing). Then clone https://github.com/Extelligence-ai/bagel, and use
> bagel's describe_data_source tool on data/sample/ros2/mcap. Report topic
> count and duration.

Grok Bot adds the server and calls it. You should see **4 topics** and a
duration of about **2.97 seconds**.

The server is added to your account, so every bot on it can use Bagel.

Other formats come as extras: ask Grok Bot to start the server with them, for
example `uvx --from "bagel-mcp[ros,px4,automotive]" bagel-mcp --transport stdio`
for ROS 1/2 bags, PX4 logs and CAN / MDF4. The
[extras list](../../../README.md#-install-with-uvx-or-pip-no-docker) has the
rest, and if an extra is missing, Bagel's error names the exact command to add it.

## 📂 Bring your own data

Grok Bot's computer can't reach your laptop. Get logs onto it by asking Grok Bot to:

- download them from a URL or object storage (for example S3) it can reach, or
- clone a repository that contains them.

Then ask questions about them by path, the same as with any other client.

Live robots (ROS 2 topics, rosbridge, MQTT on your network) aren't reachable
from Grok Bot's computer. For those, run Bagel locally with Docker.

## 🎉 Congrats! You are all set.

Still having trouble? 🤦 It's not your fault. [File a ticket](https://github.com/Extelligence-ai/bagel/issues) and let us know.
