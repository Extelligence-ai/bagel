# LeRobot: from detected events to training data (beta)

Data reduction's endgame is training-data curation: *"keep the 30 seconds around
every interesting event"* is episode selection. Bagel exports those windows as a
[LeRobotDataset v3.0](https://huggingface.co/docs/lerobot/en/lerobot-dataset-v3),
the Hugging Face robot-learning format, ready to load with `lerobot >= 0.4.0`
or push to the Hub.

## From a prompt

> Find every hard deceleration in ./flight_042, then turn each ±15s window into a
> LeRobot episode: IMU as observation.state, wheel commands as action, 10 fps.

Behind the scenes this chains `preview_pipeline` (the windows) with
`export_for_lerobot`:

```text
export_for_lerobot(
  path="./flight_042", topics=["/imu", "/cmd"],
  episodes=[{"start_seconds": 104.2, "end_seconds": 134.2}, ...],
  features={
    "observation.state": ["/imu/linear_acceleration/x", "/imu/angular_velocity/z"],
    "action": ["/cmd/wheel_torque"],
  },
  fps=10, task="drive without hard braking", name="brake study",
)
-> { "dataset": "~/.bagel/artifacts/lerobot/brake_study", "episodes": 7,
     "frames": 2107, "instructions": "Load with lerobot >= 0.4.0: ..." }
```

## What gets written

The v3.0 file-based layout: `data/chunk-000/file-000.parquet` (all episodes in one
shard, boundaries resolved through metadata), `meta/info.json` (schema, fps, path
templates), `meta/episodes/` (per-episode lengths, offsets, and statistics),
`meta/tasks.parquet`, and `meta/stats.json` (normalization statistics).

Episodes are resampled onto the uniform `fps` grid with last-observation-carried-
forward, because LeRobot consumers assume fixed-rate frames.

## Load it

```python
from lerobot.datasets import LeRobotDataset

dataset = LeRobotDataset(repo_id="you/brake-study", root="~/.bagel/artifacts/lerobot/brake_study")
sample = dataset[0]  # {'observation.state': tensor([...]), 'action': tensor([...]), ...}
```

## Beta status

Exports **load-test clean with the real `lerobot` package** (`LeRobotDataset`
returns correct tensors, shapes, and episode boundaries), and the on-disk layout
matches a reference v3 dataset on the Hub field-for-field. It stays beta until
real policies have been trained from Bagel exports. If you train from one,
please report anything odd, and anything great.

## Notes

- Tabular features only for now; camera-topics-to-MP4 is the planned follow-up.
- Format grounded against the published v3.0 spec **and** a real v3 dataset on the
  Hub (`yaak-ai/L2D-v3`): the exporter's parquet columns and metadata match its
  schemas field-for-field.
