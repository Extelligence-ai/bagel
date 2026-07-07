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

## Reduce an MCAP bag

MCAP is a first-class format: any `.mcap` file or directory of them (ros1, ros2,
protobuf, or json profiles) works in **every** Bagel image -- no ROS required. The reduce module
copies raw message records within the kept windows -- no decode/re-encode -- so it needs
no rosidl typesupport:

```yaml
tasks:
  - module: src.pipeline.tasks.reduce.mcap
    args:
      event_topic: /imu
      predicate: "\"/imu\"['linear_acceleration']['x'] < -10"
      pre_seconds: 10
      post_seconds: 10
```

(`src.pipeline.tasks.reduce.ros2.mcap` remains as a back-compat alias.)

For per-event clips instead of one reduced file, pair the snippet variant with an
`on_event` cadence — same raw passthrough, one `.mcap` per event:

```yaml
cadence:
  topic: /imu
  when:
    on_event:
      predicate: "\"/imu\"['linear_acceleration']['x'] < -10"
      debounce: {last: 2, unit: second}
tasks:
  - module: src.pipeline.tasks.snippet.mcap
    lookback: {last: 10, unit: second}
    args: {post_seconds: 10}
```

## Live edge-recording

Attach the reduction to a live stream so only event windows are ever written. Use an
`on_event` cadence with a `forward` window -- the forward window delays firing until the
post-window data has arrived, so `[event - pre, event + post]` is fully captured:

```yaml
cadence:
  topic: /imu
  when:
    on_event:
      predicate: "\"/imu\"['linear_acceleration']['x'] < -10"
      debounce: {last: 2, unit: second}
      forward: {last: 10, unit: second}   # buffer 10s past each event before firing
tasks:
  - module: src.pipeline.tasks.snippet.ros2.db3
    lookback: {last: 10, unit: second}
    args: {post_seconds: 10}
```

Pass this pipeline to `subscribe_live_topics` (via the `pipeline` argument of
`TopicSink.subscribe`); it runs on the sink's live message callback and fires once per
deceleration event, recording only the window around each.

## Batch: reduce many logs at once

Run one reduction across a whole folder or glob of bags. Each source is processed
independently (its own artifact directory), and a failure on one source is reported but
does not stop the batch. From an MCP client:

> Reduce every bag under `./logs` with this pipeline.

which calls `run_pipeline_batch(config, ["./logs/*"])` and returns a summary:

```text
{ "sources": 42, "completed": 40, "failed": 2, "artifacts": 40, "results": [ ... ] }
```

The base config's `path` is ignored — it is overridden per source. Pair this with the
upload task below to push the reduced bags to cloud storage.

## Upload the reduced data to S3

Add an upload task to the pipeline (or run one afterwards) to push artifacts to S3 or
any S3-compatible store (MinIO, Cloudflare R2, ...) via `endpoint_url`. Files whose
SHA-256 already matches the remote object are skipped, so re-runs are cheap:

```yaml
tasks:
  - module: src.pipeline.tasks.reduce.mcap
    args: { event_topic: /imu, predicate: "...", pre_seconds: 10, post_seconds: 10 }
  - module: src.pipeline.tasks.upload.s3
    args:
      bucket: drone-fleet-reduced
      source: ~/.bagel/artifacts        # file, directory, or glob
      prefix: "2026/week-27"
      filter_modified_at: true          # only files modified within the lookback window
    lookback: { last: 1, unit: hour }
```

Credentials use the standard AWS resolution chain (env vars, `~/.aws`, instance role).

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

## Verify the ROS write paths (integration tests)

The db3/MCAP reduce and snippet writers are covered by integration tests that synthesize
a bag with real IMU telemetry (two decelerations below -10) and assert on the written
output. They skip automatically outside ROS; run them in a container:

```bash
docker compose run --rm -v "$PWD:/home/ubuntu/work" ros2-jazzy bash -c '
  cd /home/ubuntu/work
  uv pip install --python /home/ubuntu/runtime/.venv/bin/python -q pytest
  source /opt/ros/jazzy/setup.bash
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
    /home/ubuntu/runtime/.venv/bin/python -m pytest test/pipeline/integration -v'
```

To generate a standalone synthetic telemetry bag (db3 or mcap) for manual experiments:

```bash
uv run python -m test.pipeline.integration.synth --directory ./data/synthetic --storage mcap
```

> [!NOTE]
> The bundled ROS2 sample (`data/sample/ros2/db3`) contains only `std_msgs/String`
> topics over a sub-microsecond span, so it is not a meaningful reduction target — use
> the synthesizer above or a bag with real numeric telemetry.
