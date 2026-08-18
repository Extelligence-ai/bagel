# Cloudini Integration

[Cloudini](https://github.com/facontidavide/cloudini) is a fast pointcloud compression library.
This integration lets you decode cloudini-compressed pointcloud topics (e.g., `CompressedPointCloud2`)
in Bagel data pipelines and write them to standard formats.

> [!TIP]
> Compressing lidar data with cloudini significantly reduces file sizes while preserving accuracy.
> We recommend keeping cloudini enabled (the default) for all pointcloud workflows.

## Prerequisites

Install the cloudini dependency group:

```bash
uv sync --group cloudini
```

This installs `wasmtime` (WebAssembly runtime) and `numpy`.

You also need the **cloudini WASM binary** (`cloudini_wasm.wasm`). Build it from the
[cloudini repo](https://github.com/facontidavide/cloudini) or use a pre-built release:

```bash
git clone https://github.com/facontidavide/cloudini.git
cd cloudini
cmake -B build/release -S cloudini_lib -DCMAKE_BUILD_TYPE=Release
cmake --build build/release --parallel
```

## Pipeline YAML Configuration

Add a `DecodePointCloudTask` to your pipeline YAML:

```yaml
name: decode_lidar
path: /data/recording.mcap
allow_failure: false

cadence:
  topic: /lidar/points
  when: once_at_end

tasks:
  - module: src.pipeline.tasks.cloudini.decode_pointcloud
    args:
      topics:
        - /lidar/points
      output_directory: /output/pointclouds
      wasm_path: /path/to/cloudini_wasm.wasm
      output_format: npz          # "npz" (default) or "csv"
```

### Task Parameters

| Parameter          | Type       | Required | Default | Description                                      |
| ------------------ | ---------- | -------- | ------- | ------------------------------------------------ |
| `topics`           | list[str]  | Yes      | —       | Pointcloud topics to decode                      |
| `output_directory` | str        | Yes      | —       | Directory to write decoded files                 |
| `wasm_path`        | str        | Yes      | —       | Path to `cloudini_wasm.wasm`                     |
| `output_format`    | str        | No       | `npz`   | Output format: `npz` or `csv`                    |
| `cloudini`         | bool       | No       | `true`  | Set to `false` to disable cloudini for this task |

## Opting Out

There are three ways to disable cloudini, depending on the scope:

### 1. Don't install the dependencies

If the `cloudini` dependency group is not installed, the task logs a warning and
skips gracefully. No error is raised.

### 2. Disable per-task in YAML

Set `cloudini: false` in the task args:

```yaml
tasks:
  - module: src.pipeline.tasks.cloudini.decode_pointcloud
    args:
      topics: [/lidar/points]
      output_directory: /output/pointclouds
      wasm_path: /path/to/cloudini_wasm.wasm
      cloudini: false   # skip decoding for this task
```

### 3. Disable globally

Set `CLOUDINI_ENABLED=false` in your `.env` file to disable cloudini across
all pipelines:

```
CLOUDINI_ENABLED=false
```

This overrides the per-task default. The setting can also be passed as an
environment variable.

## Output Formats

### NPZ (default)

Each decoded pointcloud is saved as a compressed NumPy archive (`.npz`).
The archive contains a `points` array (structured with field names like `x`, `y`, `z`,
`intensity`, etc.) and the header metadata.

```python
import numpy as np

data = np.load("lidar_points_1.234567.npz", allow_pickle=True)
points = data["points"]
print(points["x"], points["y"], points["z"])
```

### CSV

Each pointcloud is saved as a CSV file with column headers matching the
pointcloud field names.

## Compressing pointclouds

The reverse of decoding: shrink a bag's pointcloud topics for storage or transfer. The
`compress_pointcloud` task wraps cloudini's `cloudini_rosbag_converter`, converting every
`sensor_msgs/PointCloud2` topic in a ROS2 MCAP bag into the much smaller
`point_cloud_interfaces/CompressedPointCloud2`. Other topics pass through unchanged. To
analyze the compressed topics afterwards, run the `decode_pointcloud` task on the
result (see above): Bagel does not decode cloudini transparently during ingestion.

### Requirements

The `cloudini_rosbag_converter` binary must be on PATH. Build it from
[cloudini](https://github.com/facontidavide/cloudini) (see `cloudini_ros`). If the binary
is missing, or `CLOUDINI_ENABLED=false`, the task logs a message and skips rather than
failing the pipeline.

### Pipeline usage

```yaml
tasks:
  - module: src.pipeline.tasks.cloudini.compress_pointcloud
    args:
      cloudini: true # per-task opt-out; set false to skip this pipeline
```

Under the hood this runs:

```bash
cloudini_rosbag_converter -f <source.mcap> -o <artifact.mcap> -c
```

The compressed bag lands in the pipeline's artifact directory.

## Settings Reference

These settings are configured in `.env` or as environment variables:

| Setting                      | Default | Description                                        |
| ---------------------------- | ------- | -------------------------------------------------- |
| `CLOUDINI_ENABLED`           | `true`  | Global toggle for cloudini decoding                |
| `CLOUDINI_DEFAULT_RESOLUTION`| `0.001` | Reserved: not currently applied by any task. The compress task uses the converter's built-in quantization (its CLI has no resolution option) |
