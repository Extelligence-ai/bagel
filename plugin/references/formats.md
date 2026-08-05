# Format → Docker image → arguments

Start the container matching the data format, then connect (default
`http://localhost:8000/sse`).

| Data | Typical files | Compose service / image | Extra args needed |
|---|---|---|---|
| ROS 2 bag | `.mcap`, `.db3` dirs | `ros2-kilted` / `ros2-jazzy` / `ros2-iron` / `ros2-humble` (match the bag's distro) | — |
| ROS 1 bag | `.bag` | `ros1-noetic` / `ros1-noetic-cv` (latter for CV workloads) | — |
| ROS text logs | `~/.ros/log/*.log` | any ros image | — |
| PX4 | `.ulg` | `px4` | — |
| ArduPilot | `.bin` | `ardupilot` | — |
| Betaflight | `.bbl`, `.bfl` | `betaflight` | — |
| CAN capture | `.blf`, `.asc` | `iot` | **required**: `args={"dbc": "./path/to/bus.dbc"}` — the DBC is the bus schema; without it the source cannot decode |
| ASAM MDF | `.mf4` | `iot` | — |
| CSV / JSON / Parquet | files or partitioned dirs | `apache-arrow` (lightest) | optional timestamp column/format args |
| Live MQTT / rosbridge | broker or websocket | image matching the robot's stack | host/port of the broker |

Symptoms of a wrong setup: connection refused → container not running or wrong
port; typed `InvalidPathError` naming the format → wrong image or corrupt file;
"Missing required constructor arguments: dbc" → CAN without its DBC.
Changed MCP_TRANSPORT or the port? The bundled connection assumes sse on :8000 — add the server manually with claude mcp add.
