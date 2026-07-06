# Natural-language data reduction

Reduce a large log down to just the moments that matter: detect **events** with a
SQL predicate, keep a window of data around each one, and discard the rest.

There are two output modes:

| Mode        | Output                         | Use when the request says            |
| ----------- | ------------------------------ | ------------------------------------ |
| **reduce**  | one bag = union of kept windows | "discard the rest", "only keep", "reduce" |
| **snippet** | one clip per event (N bags)    | "a snippet/clip for each event"      |

Both share the same core: find rising-edge events → build `[event - pre, event + post]`
windows → merge overlaps.

## Always preview first

`preview_pipeline` is a dry run: it reports how many events were found and how much data
would be kept, and **writes nothing**. Preview, confirm, then run — the same "audit before
you act" discipline as inspecting a SQL query.

From an MCP client (e.g. Claude Code):

> Preview keeping 10s before and after every deceleration below -10 on `/imu` in
> `./data/logs/flight_042`.

which calls:

```text
preview_pipeline(
  path="./data/logs/flight_042",
  event_topic="/imu",
  predicate="\"/imu\"['linear_acceleration']['x'] < -10",
  pre_seconds=10, post_seconds=10, debounce_seconds=2,
)
-> { "event_count": 7, "kept_seconds": 92.0, "total_seconds": 1200.0,
     "kept_fraction": 0.076, "intervals": [ ... ] }
```

> 7 events, keeping 92s of 1200s (7.6%).

### The predicate contract

The topic column is a DuckDB `STRUCT`, so predicate fields are accessed as
`"<topic>"['field']['subfield']` — for example `"/imu"['linear_acceleration']['x'] < -10`.
Use `describe_topic` to find the exact field paths and units before writing a predicate.

## Run the reduction (ROS2 bag)

The reduce/snippet writers use `rosbag2`, so run them inside a ROS service from
`compose.yaml` (e.g. `ros2-jazzy`). An example pipeline lives at
[`pipelines/hard_decel_reduce.yaml`](../../pipelines/hard_decel_reduce.yaml) — edit its
`path`, `event_topic`, and `predicate` to match your bag.

```bash
docker compose run --rm ros2-jazzy \
  uv run python run.py pipelines/hard_decel_reduce.yaml --verbose
```

The reduced bag is written under the artifact directory
(`~/.bagel/artifacts/pipeline=hard_decel_reduce/...`).

Or drive it conversationally from an MCP client with `run_pipeline` (build + run a config)
and `save_pipeline` (persist a config as YAML for reuse).

## Verify the mechanism without ROS

The event → window → merge → run machinery is format-agnostic; only the bag *writer* needs
ROS. To confirm the pipeline runner works end to end on this machine, run a pipeline against
the bundled CSV sample (pure Python, no ROS):

```bash
uv run python - <<'PY'
import server
config = {
    "name": "csv_smoke",
    "site": "demo",
    "asset": "demo",
    "path": "./data/sample/pyarrow/csv/flight.csv",
    "allow_failure": False,
    "cadence": {"topic": "message", "when": "once_at_end"},
    "tasks": [{
        "module": "src.pipeline.tasks.write_topics_to_file",
        "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
        "args": {"topics": ["message"], "output_format": "csv"},
    }],
}
print(server.run_pipeline(config))
PY
```

You can also preview a reduction on the same CSV sample
(`message['accel_x'] < -10` → 2 events, ~33.6% kept), exercising the full detect/merge
path without ROS.

> [!NOTE]
> The bundled ROS2 sample (`data/sample/ros2/db3`) contains only `std_msgs/String`
> topics over a sub-microsecond span, so it is not a meaningful reduction target — point
> the reduce pipeline at a bag with real numeric telemetry.
