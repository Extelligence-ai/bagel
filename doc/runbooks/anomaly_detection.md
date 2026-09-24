# Anomaly detection with Jev (beta)

Ship only the log segments where something went wrong, each with a label saying what.

> Watch the motor current and heartbeat. When something looks off, ask Jev what it
> is, keep those 10 seconds, and upload them with the label to our bucket.

The `anomaly` gate learns what normal looks like on the robot, screens every window
against it, and asks [Jev](https://docs.typesafe.ai/models) (TypeSafe's typed-decision
model) to name anything unusual. Downstream tasks cut the slice, write a JSON label next
to it, and upload both to any bucket Bagel supports.

> **Beta.** Recorded logs only (a completed sink recording counts): the Jev call is
> synchronous and would stall a live ingest thread, so `subscribe_live_topics` refuses
> pipelines that contain this gate. The Jev backend has been run against live Jev
> through Vercel AI Gateway on a real drive (a nuScenes scene) and on a synthetic fault
> log, with the documented reply format confirmed; a direct TypeSafe key has not been
> exercised yet. **It graduates** when the reference-log baseline has shipped so
> warm-up no longer hides the start of every run, and the backend call has moved off
> the ingest thread.

## How it works

```text
every window (e.g. 10 s)
  │
  ├─ summarize ──── per-signal count/min/max/mean/std, per-topic last message
  │
  ├─ screen ─────── vs. the rolling baseline learned on this robot:
  │                  the window mean > z_threshold std devs from normal (mean_shift),
  │                  one sample far beyond what its sample count explains (z_score),
  │                  or an expected topic silent for > dropout_seconds (dropout)
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
new normal. Non-finite samples (PX4 and ROS topics carry NaN/Inf by design) are left out
of every statistic.

## Set it up

1. Get a TypeSafe API key and export it where the pipeline runs:
   `export TYPESAFE_API_KEY=...`. A missing key fails when the pipeline is built, not
   at the first anomaly. No TypeSafe key? [Vercel AI Gateway](https://vercel.com/docs/ai-gateway/sdks-and-apis/typesafe)
   serves the same API with a gateway key: set `url:
   https://ai-gateway.vercel.sh/typesafe/v1/systemone`, `model: typesafe-ai/jev` and
   `api_key_env: AI_GATEWAY_API_KEY` on the gate.
2. Start from [`pipelines/anomaly_upload.yaml`](../../pipelines/anomaly_upload.yaml):

```yaml
name: anomaly_upload
site: warehouse
asset: forklift
path: ./data/logs/shift_042          # a recorded log (live sources: after beta)
allow_failure: false
cadence:
  topic: /motor/current
  when: {every: 10, unit: second}
gates:
  - module: src.pipeline.gates.anomaly
    lookback: {last: 10, unit: second}
    args:
      topics: [/motor/current, /heartbeat]   # dropout needs a topic other than the
      anomalies:                       # cadence topic; plain language is what Jev reads
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

3. Calibrate it (next section), then run it: `uv run run.py pipelines/anomaly_upload.yaml`.

Swap `upload.s3` for `upload.gcs` or `upload.azure`, or give `upload.s3` an
`endpoint_url` for MinIO / Cloudflare R2. Anything the gate passes goes to whichever
upload task you choose.

## Calibrate first

Before saving the pipeline, dry-run the screen over a log you know. Nothing is written
and no decision model is called:

> What would the anomaly gate flag on ./shift_042 with 10 s windows, watching
> `/motor/current.value` and `/imu.linear_acceleration.x`?

That is the `preview_anomalies` tool (`calibrate()` in `src/pipeline/decide/calibrate.py`):

```text
preview_anomalies("./shift_042", window_seconds=10,
                  signals=["/motor/current.value", "/imu.linear_acceleration.x"], warmup_minutes=1)
-> { "windows": 61, "warmup_windows": 6, "screened_windows": 55,
     "flagged": [{"offset_seconds": 410.0, "reasons": [{"kind": "mean_shift", ...}, ...]}, ...],
     "flagged_fraction": 0.036, "by_signal": {"/motor/current.value": 1},
     "advice": [] }
```

Pass `cadence_topic` (and `cadence_seconds`, if the pipeline evaluates less often than
the window length) so windows end where the saved pipeline will fire. The preview is
screen-only: it models `mode: screen` with the model confirming every flag, `every: N
seconds` cadences, and a gate that runs on every fire, so list the anomaly gate first
if you combine gates.
`advice` names signals that trip the screen in most windows (they drift by design, drop
them), topics that read as dropouts every window (raise `dropout_seconds`), and a
warm-up that swallows the log. Iterate until the flags look like real events, then
write the YAML. The LLM recipe `compose/anomaly_pipeline` walks these steps.

## Settings

| Arg | Default | What it does |
| --- | --- | --- |
| `anomalies` | *(required)* | Named anomaly types and a plain-language description of each. `normal`, `other_unusual` and `screen_only` are reserved. |
| `topics` | all | Topics to watch. |
| `signals` | all numeric fields except `header`/`stamp` | Exact dotted signals, e.g. `/imu.linear_acceleration.x`. ROS header timestamps are skipped by default because they grow every message. `[]` watches no signals (dropouts only). **Watch rates and errors, not states:** accelerations, angular rates, currents, lane offset. Positions, velocities and orientations drift by design as the robot moves, so "far from the baseline mean" says nothing about them (on a nuScenes drive, quaternion fields flagged every window after the first turn). |
| `max_signals` | `64` | Refuse to watch more signals than this. Each adds to the summary query and to the request Jev reads (64k-token context); a PX4 log exposes ~2,000, so pick `topics` or `signals`. |
| `mode` | `screen` | `screen`: ask Jev only about windows the on-robot check flags. `always`: ask about every window (more calls, catches what the screen misses). |
| `z_threshold` | `3.0` | Flag a window whose mean is this many baseline std devs from normal. A single sample must clear this plus the extreme its sample count explains (about 3σ more at 50 Hz), so noisy signals don't trip every window. |
| `dropout_seconds` | `2` | Flag an expected topic silent this long at the window's end, measured from its last message even across windows (so a threshold longer than the window waits for it). Expected = publishes in most baseline windows, so event-driven topics don't count. The cadence topic can never drop out: the pipeline only runs when it publishes. |
| `baseline_window_minutes` | `30` | How much recent history defines "normal". |
| `warmup_minutes` | `5` | History needed before screening starts; in screen mode nothing is flagged before then (in `always` mode Jev is still asked, without baseline statistics). Must not exceed `baseline_window_minutes`. |
| `min_probability` | `0.6` | Confidence Jev needs for its label to count; less confident answers count as normal. |
| `question` | built in | The instructions Jev receives. |
| `backend` | `jev` | `jev` (TypeSafe), `remote` (any endpoint answering `{"probabilities": {...}}`) or `local` (model on the robot, see below). |
| `model` | `jev-latest` | Pin a version such as `jev-1.13.0` for reproducible labels. |
| `url` | TypeSafe | Override the endpoint, e.g. a LiteLLM pass-through proxy. Must be https unless the host is loopback (`localhost`/`127.0.0.1` inside the container; `host.docker.internal` is not), because the key travels as a bearer token. Redirects are never followed. |
| `api_key_env` | `TYPESAFE_API_KEY` | Environment variable holding the key. Compose forwards only `TYPESAFE_API_KEY` into the containers; for another name, start with `docker compose run -e OTHER_KEY ...` or add it to the service's `environment`. |
| `timeout_seconds` | `10` | Per-request timeout. |

## The label file

`write_annotations` writes `<timestamp>.json` under `task=write_annotations/`, named
after the same timestamp as the slice under `task=snip_mcap/`. Each gate's record sits
under the gate's name, so two gates never overwrite each other's fields. This one is a
real record (rounded) from a test run with a planted current spike:

```json
{
  "asof_seconds": 1700000410.0,
  "anomaly": {
  "label": "overcurrent",
  "probabilities": {"overcurrent": 0.9, "sensor_dropout": 0.033, "other_unusual": 0.033, "normal": 0.033},
  "verified": true,
  "model": "jev-1.13.0",
  "mode": "screen",
  "screen_reasons": [
    {"kind": "mean_shift", "signal": "/motor/current.value", "value": 2.6, "z": 45.3},
    {"kind": "z_score", "signal": "/motor/current.value", "value": 9.0, "z": 226.3}
  ],
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
}
```

`screen_reasons` says what tripped the on-robot check (`mean_shift`, `z_score` or
`dropout`); `baseline` is what "normal" meant at that moment.

## When Jev can't be reached

Timeouts, rate limits, dropped connections, HTTP errors and malformed replies never
crash the pipeline.
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

Other images fail at pipeline build with a message pointing at this flag. The
`ros2-jazzy-jev` service reserves the host's NVIDIA GPUs, so it needs the NVIDIA
container runtime; the local backend then runs on CUDA, and on CPU when PyTorch sees
no GPU (PyPI wheels on Jetson are CPU-only). Hosts without the runtime should use a
plain image with the hosted `jev` backend. It scores each choice with a generic causal LM; it does not load
Open-Jev's decision head.

## Asking your own question: the `decide` gate

For a single typed question without a baseline (*"upload, keep_local or discard?"*),
use `src.pipeline.gates.decide`: `question`, `choices`, the `accept` list that opens
the gate, `min_probability`, and the same `backend` options (default `remote`). It
also hands its decision to `write_annotations`.

## Limits

- **Beta:** recorded logs only; live Jev exercised through Vercel AI Gateway, not yet
  with a direct TypeSafe key.
- **Dropouts are judged at the window's end.** A topic that went quiet for 5 s in the
  middle of a 10 s window and came back is not a dropout.
- **The baseline is per run.** It is learned from the data the pipeline sees and
  resets when the pipeline restarts; in screen mode the first `warmup_minutes` are never
  flagged.
  Known-good reference logs and fleet baselines pushed from Matcha are planned.
- **Slow drift can hide.** A signal that creeps up over longer than
  `baseline_window_minutes` moves the baseline with it.
- **Uploads are per artifact directory.** Each passing window runs the upload task over
  the pipeline's artifact directory; files already in the bucket are skipped by
  checksum.
