# Format → how to run Bagel → arguments

Two ways to run the server; the data decides.

**No Docker (`uvx`)** for every recorded format: the MCP client launches the
server over stdio (`{"command": "uvx", "args": ["bagel-mcp", "--transport",
"stdio"]}`; Claude Code: `claude mcp add bagel -- uvx bagel-mcp --transport
stdio`; Codex: `codex mcp add bagel -- uvx bagel-mcp --transport stdio`). Add the
format's extra with `uvx --from "bagel-mcp[<extra>]" bagel-mcp --transport
stdio`; `ros` covers `.bag` and `.db3` bags in pure Python. Live rosbridge needs
Docker; the server names the image if asked for one.

**Docker** for everything: start the container matching the data format, then
connect (default `http://localhost:8000/mcp`). Codex: `codex mcp add bagel --url
http://localhost:8000/mcp` once. Claude Code installs connect automatically.

| Data | Typical files | pip extra (`uvx`) | Compose service (Docker) | Extra args needed |
|---|---|---|---|---|
| ROS 2 bag, MCAP storage | `.mcap` dirs with `metadata.yaml` | none (Bagel's own MCAP reader) | `ros2-kilted` / `ros2-jazzy` / `ros2-iron` / `ros2-humble` | none |
| ROS 2 bag, sqlite storage | `.db3` dirs | `ros` | `ros2-kilted` / `ros2-jazzy` / `ros2-iron` / `ros2-humble` (match the bag's distro) | none |
| ROS 1 bag | `.bag` | `ros` | `ros1-noetic` (`ros1-noetic-cv` for image topics) | none |
| ROS text logs | `~/.ros/log/*.log` | none | any ros image | none |
| PX4 | `.ulg` | `px4` | `px4` | none |
| ArduPilot | `.bin` | `ardupilot` | `ardupilot` | none |
| Betaflight | `.bbl`, `.bfl` | `betaflight` | `betaflight` | `.bbl` from a flash chip holds several flights: `args={"log_index": 1}` picks one |
| CAN capture | `.blf`, `.asc` | `automotive` | any image, Bagel ≥ 2.4.2 (beta; `apache-arrow` is lightest) | **required**: `args={"dbc": "./path/to/bus.dbc"}` — the DBC is the bus schema; without it the source cannot decode |
| ASAM MDF | `.mf4` | `automotive` | any image, Bagel ≥ 2.4.2 (beta; `apache-arrow` is lightest) | none |
| Copper (copper-rs) | app-exported `.mcap` (a raw `.copper` log must be exported by its app's log extractor first; Bagel's error message walks you through it) | none | `apache-arrow` (lightest) | none |
| CSV / JSON / Parquet | files or partitioned dirs | none | `apache-arrow` (lightest) | optional timestamp column/format args |
| Live MQTT (incl. Sparkplug B) | broker | `iot` | `iot` | host/port of the broker |
| Live rosbridge | websocket | Docker only | image matching the robot's ROS stack | host/port of the bridge |

Symptoms of a wrong setup: connection refused → container not running or wrong
port; "needs ROS bag support" → a `.db3`/`.bag` on a `uvx` install without the
`ros` extra (`uvx --from "bagel-mcp[ros]" ...`); a typed error naming the format → wrong image or corrupt file;
"Missing required constructor arguments: dbc" → CAN without its DBC;
"No module named 'asammdf'" or "'can'" → an image older than 2.4.2, so pull `:latest`.
