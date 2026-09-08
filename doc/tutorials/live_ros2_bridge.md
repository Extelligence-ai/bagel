# Tutorial: chat with a live ROS2 robot over rosbridge

In this tutorial you'll connect Bagel to a **running ROS2 system** and ask questions
about its live topic data: no bag files involved. Bagel subscribes through a
[rosbridge](https://github.com/RobotWebTools/rosbridge_suite) websocket, buffers
messages locally, and answers questions over the buffer with deterministic SQL.

Every command below was verified end to end against a live rosbridge.

## What you need

- A ROS2 system (robot, simulator, or the demo publisher below) with
  `rosbridge_suite` available
- Bagel running per the [Quickstart](../../README.md#%EF%B8%8F-quickstart), with an
  MCP client (e.g. Claude Code) connected

## Step 1 · start rosbridge next to your robot

On the machine running ROS2:

```bash
sudo apt install ros-${ROS_DISTRO}-rosbridge-suite   # if not already installed
ros2 launch rosbridge_server rosbridge_websocket_launch.xml
```

Wait for: `Rosbridge WebSocket server started on port 9090`.

**No robot handy?** Run a demo publisher in Bagel's own image: rosbridge is
pre-installed:

```bash
docker run -d --name demo-robot -p 9090:9090 \
  ghcr.io/extelligence-ai/bagel/ros2-jazzy:latest bash -c '
    source /opt/ros/jazzy/setup.bash
    ros2 launch rosbridge_server rosbridge_websocket_launch.xml &
    sleep 5
    ros2 topic pub -r 5 /imu sensor_msgs/msg/Imu "{linear_acceleration: {x: -2.0, z: 9.8}}"'
```

## Step 2 · discover the live topics

In your MCP client, prompt:

> List the available live topics from the ROS2 bridge.

Bagel calls `list_live_topics("ros2.bridge")` and reports what's publishing: for the
demo robot you'll see `/imu` (plus `/rosout` and `/parameter_events`). If your bridge
runs on another machine, add "on host `<hostname>`" to the prompt; from inside the
Bagel container, the host machine is `host.docker.internal` (the default).

## Step 3 · subscribe

> Subscribe to `/imu` from the ROS2 bridge.

Bagel calls `subscribe_live_topics("ros2.bridge", topics=["/imu"])`, starts buffering
messages to a local **sink directory**, and returns its path. Message structure comes
from the bridge's type definitions, so nested fields (e.g.
`linear_acceleration.x`) are fully typed.

Leave it running: the buffer fills as the robot publishes.

## Step 4 · ask questions about the live data

The sink directory is a normal Bagel data source. Prompt, for example:

> What's the minimum `linear_acceleration.x` on `/imu` in the buffered data so far?

Under the hood that's a deterministic DuckDB query like:

```sql
SELECT MIN("/imu"['linear_acceleration']['x']) FROM "/imu"
```

Ask again later and the answer reflects everything buffered since you subscribed.

## Step 5 · going further: react to live events

Bagel's event-driven pipelines can run on live messages too: for example, *"every
time deceleration exceeds -10, keep 10s before and after"*, including a `debounce`
so an oscillating signal counts as one event, and a `forward` window so the post-event
data is captured before anything fires. See the **data reduction runbook**
(`doc/runbooks/data_reduction.md`) for the pipeline configuration.

## Troubleshooting

- **`Connection refused`**: check the bridge is up (`Rosbridge WebSocket server
  started on port 9090`) and the port is reachable from where Bagel runs
  (`nc -z <host> 9090`).
- **Bagel runs in Docker, bridge on your host**: the default host
  `host.docker.internal` handles this; pass an explicit `host` for anything else.
- **Topic missing from the list**: rosbridge only reports topics with active
  publishers; confirm with `ros2 topic list` on the robot.
- **High-rate topics**: subscription supports `throttle_rate` and `queue_length`
  args to keep buffering manageable.

Clean up the demo robot with `docker rm -f demo-robot`.
