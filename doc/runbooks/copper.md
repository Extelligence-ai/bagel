# Copper (copper-rs) robot logs

[Copper](https://github.com/copper-project/copper-rs) is a Rust robotics runtime
that records everything into a unified binary log (`.copper`). Those logs are
decoded with the generating application's compile-time types, so Bagel does not
read them directly: instead, every Copper app can export its log to MCAP with
JSON-encoded channels and jsonschema schemas, which Bagel's generic MCAP path
ingests natively. One channel per task, fully typed columns, no ROS required
(the `apache-arrow` container is enough).

## Start Bagel (no ROS image needed)

Copper's MCAP exports go through Bagel's generic Arrow path, so the lightest
container is enough:

```bash
git clone https://github.com/Extelligence-ai/bagel.git && cd bagel
docker compose run --service-ports apache-arrow
```

Connect your MCP client to `http://localhost:8000/sse` (for Claude Code:
`claude mcp add --transport sse bagel http://localhost:8000/sse`; the bundled
plugin wires this automatically). Then try the committed sample before your
own logs:

> Summarize ./data/sample/copper/imu_probe.mcap

## One-time setup in your Copper app

Copper apps conventionally ship a log-extractor binary next to the app. Three
things are needed for MCAP export, and the last two are easy to miss:

1. A logreader binary (most Copper templates already have one):

```rust
// src/logreader.rs
pub mod tasks;
use cu29::prelude::*;
use cu29_export::{run_cli, trace_type_to_jsonschema};

gen_cumsgs!("copperconfig.ron");

// MCAP export needs a JSON schema per task output. `get_all_task_ids()`
// includes every task (sinks too), so map each id to its payload type.
impl PayloadSchemas for cumsgs::CuStampedDataSet {
    fn get_payload_schemas() -> Vec<(&'static str, String)> {
        <cumsgs::CuStampedDataSet as MatchingTasks>::get_all_task_ids()
            .iter()
            .map(|&id| {
                let schema = match id {
                    "imu" => trace_type_to_jsonschema::<ImuReading>(),
                    "filter" | "sink" => trace_type_to_jsonschema::<FilteredImu>(),
                    other => panic!("no schema registered for task {other}"),
                };
                (id, schema)
            })
            .collect()
    }
}

fn main() {
    run_cli::<CuMsgs>().expect("Failed to run the export CLI");
}
```

2. The `mcap` cargo feature on `cu29-export` (it is **not** on by default):

```toml
[features]
logreader = ["dep:cu29-export", "cu29-export/mcap"]
```

## Export and query

```bash
# In your Copper project:
cargo run --features logreader --bin <your-app>-logreader -- \
    logs/robot.copper export-mcap --output robot.mcap
```

Then point Bagel at `robot.mcap` like any other MCAP:

> Is my IMU overheating in ./robot.mcap?

```sql
SELECT COUNT(*) AS samples,
       MAX("/filter".payload.temperature_c) AS max_temp_c,
       SUM(CASE WHEN "/filter".payload.overheating THEN 1 ELSE 0 END) AS hot
FROM "/filter"
```

## Notes

- **Message shape**: Copper wraps every message as
  `{payload, tov, process_time, status_txt}`, so your fields live under
  `topic.payload.<field>`. `tov` (time of validity) carries the sensor-side
  timestamps in nanoseconds.
- **Timestamps are robot-clock**: Copper's clock starts near zero at app
  startup, so `timestamp_seconds` is time since boot, not wall-clock epoch.
  Time windows and ordering work as usual; absolute-time questions do not.
- **`__meta` channels**: each task also gets a `<task>/__meta` channel carrying
  iterations where that task produced no payload (e.g. sinks). They are safe to
  ignore for analysis.
- **Raw `.copper` files**: Bagel recognizes them by magic bytes and raises an
  error pointing back to this workflow. Real logs are slab families
  (`robot_0.copper`, `robot_1.copper`, ...) with `robot.copper` symlinked to
  the first slab; the logreader takes the base name and finds the rest.

A reference sample produced by a real Copper app (synthetic IMU pipeline) is
committed at `data/sample/copper/imu_probe.mcap` and exercised by
`test/pipeline/test_copper_mcap.py`.

This whole workflow is verified against copper-rs's own `cu_caterpillar`
example end to end: an 8 second run produced a 2.3 GB slab family, exported to
a 19 GB MCAP (90 million messages, 17 channels), and Bagel summarized it in
under 2 seconds and answered windowed SQL over it. Two reproduction notes:
a fresh copper-rs clone expects sibling repos checked out next to it to build,
and the caterpillar logreader needs only the `mcap` feature of `cu29-export`
(the `python` feature wants a linkable libpython).
