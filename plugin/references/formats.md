# Format → Docker image → arguments

Start the container matching the data format, then connect (default
`http://localhost:8000/sse`).

| Data | Typical files | Compose service | Extra args needed |
|---|---|---|---|
| ROS 2 bag | `.mcap`, `.db3` dirs | `ros2-kilted` / `ros2-jazzy` / `ros2-iron` / `ros2-humble` (match the bag's distro) | none |
| ROS 1 bag | `.bag` | `ros1-noetic` (`ros1-noetic-cv` for image topics) | none |
| ROS text logs | `~/.ros/log/*.log` | any ros image | none |
| PX4 | `.ulg` | `px4` | none |
| ArduPilot | `.bin` | `ardupilot` | none |
| Betaflight | `.bbl`, `.bfl` | `betaflight` | none |
| CAN capture | `.blf`, `.asc` | host install: `uv sync --group automotive` (beta; no dedicated image yet) | **required**: `args={"dbc": "./path/to/bus.dbc"}` — the DBC is the bus schema; without it the source cannot decode |
| ASAM MDF | `.mf4` | host install: `uv sync --group automotive` (beta) | none |
| Copper (copper-rs) | app-exported `.mcap` (a raw `.copper` log must be exported by its app's log extractor first; Bagel's error message walks you through it) | `apache-arrow` (lightest) | none |
| CSV / JSON / Parquet | files or partitioned dirs | `apache-arrow` (lightest) | optional timestamp column/format args |
| Live MQTT (incl. Sparkplug B) | broker | `iot` | host/port of the broker |
| Live rosbridge | websocket | image matching the robot's ROS stack | host/port of the bridge |

Symptoms of a wrong setup: connection refused → container not running or wrong
port; a typed error naming the format → wrong image or corrupt file;
"Missing required constructor arguments: dbc" → CAN without its DBC.
