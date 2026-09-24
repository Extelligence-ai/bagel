# Anomaly detection with Jev (beta)

Ship only the log segments where something went wrong, each with a label saying what.

> Watch the motor current and heartbeat. When something looks off, ask Jev what it
> is, keep those 10 seconds, and upload them with the label to our bucket.

The `anomaly` gate learns what normal looks like on the robot, screens every window
against it, and asks [Jev](https://docs.typesafe.ai/models) (TypeSafe's typed-decision
model) to name anything unusual. Downstream tasks cut the slice, write a JSON label next
to it, and upload both to any bucket Bagel supports.

> **Beta.** The Jev backend follows TypeSafe's documented `/v1/systemone` request and
> response format and is tested against a stand-in server; it has not yet been run
> against the live TypeSafe API. **It graduates** when a pipeline has run against live
> Jev on a real robot log with the label format confirmed, and the reference-log
> baseline has shipped so warm-up no longer hides the start of every run.

## How it works

```text
every window (e.g. 10 s)
  │
  ├─ summarize ──── per-signal count/min/max/mean/std, per-topic last message
  │
  ├─ screen ─────── vs. the rolling baseline learned on this robot:
  │                  a value > z_threshold std devs from normal, or a topic silent
  │                  for > dropout_seconds
  │
  ├─ ask Jev ────── only for screened windows (or every window with mode: always):
  │                  "which of these anomalies is it?"
  │                  → one of your named types, other_unusual, or normal
  │
  └─ pass? ──────── yes unless Jev says normal (or isn't confident enough)
        │
        ├─ snippet.mcap        cut the window
        ├─ write_annotations   JSON label next to it
        └─ upload.*            S3 / GCS / Azure / MinIO / R2
```

Windows that pass are kept out of the baseline, so an ongoing fault never becomes the
new normal.

## Set it up

1. Get a TypeSafe API key and export it where the pipeline runs:
   `export TYPESAFE_API_KEY=...`. A missing key fails when the pipeline is built, not
   at the first anomaly.
2. Start from [`pipelines/anomaly_upload.yaml`](../../pipelines/anomaly_upload.yaml):

```yaml
name: anomaly_upload
site: warehouse
asset: forklift
path: ./data/logs/shift_042          # a log, or a live source
allow_failure: false
cadence:
  topic: /motor/current
  when: {every: 10, unit: second}
gates:
  - module: src.pipeline.gates.anomaly
    lookback: {last: 10, unit: second}
    args:
      topics: [/motor/current, /heartbeat]
      anomalies:                       # plain language: this is what Jev reads
        overcurrent: "motor current far above normal"
        sensor_dropout: "a topic stops publishing"
        stall: "velocity commanded but the wheels are not moving"
      backend: jev
tasks:
  - module: src.pipeline.tasks.snippet.mcap
    lookback: {last: 10, unit: second}
  - module: src.pipeline.tasks.write_annotations
  - module: src.pipeline.tasks.upload.s3
    args:
      bucket: my-robot-anomalies
      prefix: anomalies
      source: /home/ubuntu/.bagel/artifacts/pipeline=anomaly_upload
```

3. Run it: `uv run run.py pipelines/anomaly_upload.yaml`, or stand it up at the edge
   like any other [pipeline](./pipelines.md#the-lifecycle).

Swap `upload.s3` for `upload.gcs` or `upload.azure`, or give `upload.s3` an
`endpoint_url` for MinIO / Cloudflare R2. Anything the gate passes goes to whichever
upload task you choose.

## Settings

| Arg | Default | What it does |
| --- | --- | --- |
| `anomalies` | *(required)* | Named anomaly types and a plain-language description of each. `normal`, `other_unusual` and `screen_only` are reserved. |
| `topics` | all | Topics to watch. |
| `signals` | all numeric fields except `header`/`stamp` | Exact dotted signals, e.g. `/imu.linear_acceleration.x`. ROS header timestamps are skipped by default because they grow every message. |
| `mode` | `screen` | `screen`: ask Jev only about windows the on-robot check flags. `always`: ask about every window (more calls, catches what the screen misses). |
| `z_threshold` | `3.0` | Flag a signal whose most extreme value is this many baseline std devs from its mean. |
| `dropout_seconds` | `2` | Flag a topic that has been silent this long. |
| `baseline_window_minutes` | `30` | How much recent history defines "normal". |
| `warmup_minutes` | `5` | History needed before screening starts; nothing is flagged before then. |
| `min_probability` | `0.6` | Confidence Jev needs for its label to count; less confident answers count as normal. |
| `question` | built in | The instructions Jev receives. |
| `backend` | `jev` | `jev` (TypeSafe), `remote` (any endpoint answering `{"probabilities": {...}}`) or `local` (model on the robot, see below). |
| `model` | `jev-latest` | Pin a version such as `jev-1.13.0` for reproducible labels. |
| `url` | TypeSafe | Override the endpoint, e.g. a LiteLLM pass-through proxy. |
| `api_key_env` | `TYPESAFE_API_KEY` | Environment variable holding the key. |
| `timeout_seconds` | `10` | Per-request timeout. |

## The label file

`write_annotations` writes `<timestamp>.json` under `task=write_annotations/`, named
after the same timestamp as the slice under `task=snip_mcap/`. This one is a real
record (rounded) from a test run with a planted current spike:

```json
{
  "asof_seconds": 1700000410.0,
  "label": "overcurrent",
  "probabilities": {"overcurrent": 0.9, "sensor_dropout": 0.033, "other_unusual": 0.033, "normal": 0.033},
  "verified": true,
  "model": "jev-1.13.0",
  "mode": "screen",
  "screen_reasons": [{"kind": "z_score", "signal": "/motor/current.value", "value": 9.0, "z": 226.3}],
  "window": {"start_seconds": 1700000400.0, "end_seconds": 1700000410.0, "messages": 152},
  "baseline": {
    "span_seconds": 310.0,
    "signals": {
      "/motor/current.value": {"count": 3030, "mean": 1.0, "std": 0.035},
      "/heartbeat.value": {"count": 1530, "mean": 1.0, "std": 0.0}
    },
    "topics": ["/heartbeat", "/motor/current"]
  },
  "beta": true
}
```

`screen_reasons` says what tripped the on-robot check (`z_score` or `dropout`);
`baseline` is what "normal" meant at that moment.

## When Jev can't be reached

Timeouts, rate limits, HTTP errors and malformed replies never crash the pipeline.
A window the screen flagged is still kept and uploaded, labelled `screen_only` with
`verified: false` and no model, so nothing is lost while the robot is offline or
TypeSafe is busy. Windows the screen didn't flag are dropped as usual.

## CPU-only robots and the on-robot model

The `jev` and `remote` backends only make HTTP calls, so they work in every Bagel
image, including CPU-only robots. Nothing extra is installed.

`backend: local` runs a Hugging Face model on the robot instead (`model:` is a Hub id
or local path). It needs PyTorch, which only images built with the `JEV_MODE` flag
include:

```bash
docker compose build ros2-jazzy-jev          # prebuilt variant
docker compose build <service> --build-arg JEV_MODE=true   # any other image
```

Other images fail at pipeline build with a message pointing at this flag. The local
backend scores each choice with a generic causal LM; it does not load Open-Jev's
decision head.

## Asking your own question: the `decide` gate

For a single typed question without a baseline (*"upload, keep_local or discard?"*),
use `src.pipeline.gates.decide`: `question`, `choices`, the `accept` list that opens
the gate, `min_probability`, and the same `backend` options (default `remote`). It
also hands its decision to `write_annotations`.

## Limits

- **Beta:** not yet validated against the live TypeSafe API.
- **The baseline is per run.** It is learned from the data the pipeline sees and
  resets when the pipeline restarts; the first `warmup_minutes` are never flagged.
  Known-good reference logs and fleet baselines pushed from Matcha are planned.
- **Slow drift can hide.** A signal that creeps up over longer than
  `baseline_window_minutes` moves the baseline with it.
- **Uploads are per artifact directory.** Each passing window runs the upload task over
  the pipeline's artifact directory; files already in the bucket are skipped by
  checksum.
