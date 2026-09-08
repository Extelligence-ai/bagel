# Natural-language pipelines

Every pipeline starts as a sentence:

> Every time the drone decelerates harder than -10 m/s², keep 10 seconds before and
> after. Drop everything else.

Bagel turns that into a small, auditable config: *when* to fire, *whether* to proceed,
*what* to do. It previews the effect before writing a byte, and can then run it once,
across a whole fleet, or standing at the edge. Reduction is the flagship use, but it's
one task among many: the same machinery exports PlotJuggler sessions, computes summaries
on a schedule, and ships artifacts to the cloud.

## Anatomy of a pipeline

```yaml
name: hard_decel_reduce
site: warehouse          # labels: where and what this ran on
asset: forklift
path: ./data/logs/flight_042
cadence:                 # WHEN the pipeline fires
  topic: /imu
  when: once_at_end
gates: []                # WHETHER to proceed once fired (optional)
tasks:                   # WHAT to do
  - module: src.pipeline.tasks.reduce.ros2.db3
    args:
      event_topic: /imu
      predicate: "\"/imu\"['linear_acceleration']['x'] < -10"
      pre_seconds: 10
      post_seconds: 10
      debounce_seconds: 2
```

Artifacts land under `~/.bagel/artifacts/pipeline=<name>/...`, tagged with `site` and
`asset` so fleet output stays sorted.

### Cadence · when it fires

| `when` | Fires | Example sentence |
| --- | --- | --- |
| `once_at_end` | Once, at the end of the source (or when a live stream stops) | "After the flight, ..." |
| `{every: 5, unit: minutes}` | On a fixed schedule along the topic's timeline | "Every 5 minutes, ..." |
| `{on_event: {predicate: ...}}` | On the rising edge of a SQL predicate, with optional `debounce` and `forward` windows | "Whenever the battery dips below 20%, ..." |

`on_event` fires once per transition: a condition that stays true for 400 messages is
one event, and `debounce` coalesces bursts. On live streams, `forward` waits long enough
to capture the post-event window before firing.

### Predicates · the one contract to learn

Topic columns are DuckDB `STRUCT`s, so fields are addressed as
`"<topic>"['field']['subfield']`:

```sql
"/imu"['linear_acceleration']['x'] < -10
```

Ask `describe_topic` for the exact field paths and units first, or just describe the
event in words and let your LLM write the predicate; that's the point.

### Gates and tasks · what it does

A **gate** decides whether a fired pipeline proceeds (e.g. `SqlQuery`: run a boolean SQL
check at the fire timestamp). **Tasks** do the work. Ask Bagel to
*"list the pipeline capabilities"* (`list_pipeline_capabilities`) for the live catalog on
your install; today it includes:

| Task (`src.pipeline.tasks.`…) | What it does |
| --- | --- |
| `reduce.mcap`, `reduce.ros2.mcap`, `reduce.ros2.db3`* | Rewrite the bag keeping only event windows |
| `snippet.mcap`, `snippet.ros1.bag`* | Cut a standalone snippet around the fire timestamp |
| `topic_sql` | Run SQL over a topic and write the result to a file |
| `write_topics_to_file` | Dump topics to CSV/Parquet/JSON |
| `generate_gif` | Render an image topic into a GIF |
| `cloudini.decode_pointcloud` | Decode compressed pointclouds mid-pipeline |
| `upload.s3`, `upload.gcs`, `upload.azure` | Ship artifacts to the cloud, skipping files already there |
| `notify.slack` | Post to a Slack (or compatible) webhook, with `{asset}`-style message templates |
| `rsync_files`, `send_email` | Pull files in; send results out |

\* the `ros2.db3` and `ros1.bag` writers need rosbag CLIs, so they show up inside the
ROS compose services.

Tasks chain: this is one standing pipeline on a live stream:

> Whenever the forklift brakes harder than -10 m/s², cut a ±10s snippet, upload it
> to S3, and post "🚨 hard brake on {asset} at t={asof_seconds}" to the ops channel.

## The lifecycle

1. **Preview** · `preview_pipeline` is a dry run: events found, seconds kept, nothing
   written. Same discipline as auditing SQL before trusting the math.
2. **Run** · `run_pipeline` builds and executes the config, returning artifact paths.
3. **Save** · `save_pipeline` persists it as YAML (like
   [`pipelines/hard_decel_reduce.yaml`](../../pipelines/hard_decel_reduce.yaml)) for
   review, version control, and `run.py pipelines/<name>.yaml`.
4. **Scale out** · `run_pipeline_batch` runs one config over globs of sources
   (`logs/*.mcap`), isolating failures per source and returning a combined report.
5. **Stand it up** · attach a pipeline to a live subscription
   (`subscribe_live_topics`), or list it in `STARTUP_PIPELINES_FILE` so the edge
   container re-establishes it on every restart: record continuously, keep only what
   matters.

## Go deeper

- [Event-driven data reduction](./data_reduction.md) · the flagship use, end to end
- [PlotJuggler](./plotjuggler.md) · pre-framed sessions from pipeline outputs
- [MQTT](./iot_mqtt.md) · standing pipelines on live IoT streams
