"""Export event windows as a LeRobotDataset v3.0 for robot-learning training.

Data reduction's endgame is training-data curation: "keep the 30 seconds around
every interesting event" is episode selection. This module writes those windows in
the LeRobotDataset v3.0 layout (lerobot >= 0.4.0): file-based Parquet shards with
episode boundaries resolved through metadata, exactly as documented at
https://huggingface.co/docs/lerobot/en/lerobot-dataset-v3 and validated against a
real v3 dataset on the Hub (yaak-ai/L2D-v3).

Each episode is resampled onto a uniform ``fps`` grid (last observation carried
forward via ASOF join), because LeRobot consumers assume fixed-rate frames.
Tabular features only; camera-to-MP4 export is a planned follow-up.
"""

import json
import pathlib
import re
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from settings import settings
from src.pipeline import flatten as flatten_module
from src.pipeline.plotjuggler import _numeric_columns
from src.source.postgres import quote_identifier

CODEBASE_VERSION = "v3.0"
CHUNKS_SIZE = 1000
DATA_FILES_SIZE_MB = 100
VIDEO_FILES_SIZE_MB = 500


def _resample(
    flat: duckdb.DuckDBPyRelation,
    signals: list[str],
    start_seconds: float,
    end_seconds: float,
    fps: int,
) -> list[dict[str, float]]:
    """Sample the flattened relation on a uniform fps grid, carrying values forward."""
    selects = ", ".join(f"flat.{quote_identifier(s)}" for s in signals)
    ts = quote_identifier(settings.TIMESTAMP_SECONDS_COLUMN_NAME)
    frames = int((end_seconds - start_seconds) * fps) + 1
    query = (
        f"WITH grid AS (SELECT {start_seconds} + (i / {fps}::DOUBLE) AS grid_t "  # noqa: S608 -- identifiers quoted, values are typed numbers
        f"FROM generate_series(0, {frames - 1}) AS t(i)) "
        f"SELECT grid.grid_t, {selects} FROM grid "
        f"ASOF LEFT JOIN flat_view AS flat ON grid.grid_t >= flat.{ts} "
        f"ORDER BY grid.grid_t"
    )
    rows = flat.query("flat_view", query).fetchall()
    return [dict(zip(["grid_t", *signals], row, strict=True)) for row in rows]


def export_episodes(  # noqa: PLR0913, C901 -- one pass per episode plus the metadata writers
    relation_for_window: Any,  # noqa: ANN401 -- callable(start, end) -> DuckDBPyRelation
    episodes: list[dict[str, float]],
    features: dict[str, list[str]],
    fps: int,
    task: str,
    name: str,
    robot_type: str = "unknown",
    output_directory: str | pathlib.Path | None = None,
) -> dict[str, Any]:
    """Write episode windows as a LeRobotDataset v3.0 directory.

    Args:
        relation_for_window: Callable returning a standard Bagel relation for a
            [start_seconds, end_seconds] window.
        episodes: Windows, each ``{"start_seconds": ..., "end_seconds": ...}``.
        features: Maps LeRobot feature names (e.g. "observation.state", "action")
            to lists of flattened signal names composing that feature vector.
        fps: The uniform frame rate episodes are resampled to.
        task: Natural-language task description for all episodes.
        name: Dataset name (used for the output directory).
        robot_type: Recorded into meta/info.json.
        output_directory: Where to write. Defaults to
            `<ARTIFACT_DIRECTORY>/lerobot/<name>`.

    Returns:
        ``{"dataset", "episodes", "frames", "features", "instructions"}``.

    Raises:
        ValueError: If a requested signal does not exist, no episodes are given, or
            a window contains no data.

    """
    if not episodes:
        raise ValueError("At least one episode window is required.")
    if not features:
        raise ValueError("A features mapping (e.g. {'observation.state': [...]}) is required.")

    name = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "dataset"
    directory = (
        pathlib.Path(output_directory)
        if output_directory
        else pathlib.Path(settings.ARTIFACT_DIRECTORY) / "lerobot" / name
    )
    (directory / "data" / "chunk-000").mkdir(parents=True, exist_ok=True)
    (directory / "meta" / "episodes" / "chunk-000").mkdir(parents=True, exist_ok=True)

    frame_rows: list[dict[str, Any]] = []
    episode_records: list[dict[str, Any]] = []
    global_index = 0

    for episode_index, window in enumerate(episodes):
        start, end = float(window["start_seconds"]), float(window["end_seconds"])
        flat = flatten_module.flatten(relation_for_window(start, end))
        available = set(_numeric_columns(flat))
        wanted = [signal for group in features.values() for signal in group]
        unusable = sorted(set(wanted) - available)
        if unusable:
            raise ValueError(
                f"Unknown or non-numeric signals: {unusable}. Available: {sorted(available)}"
            )

        sampled = _resample(flat, wanted, start, end, fps)
        if not sampled or all(row[wanted[0]] is None for row in sampled):
            raise ValueError(f"Episode {episode_index} [{start}, {end}] contains no data.")

        first_frame = global_index
        for frame_index, row in enumerate(sampled):
            record: dict[str, Any] = {}
            for feature, group in features.items():
                values = [float(row[s]) if row[s] is not None else 0.0 for s in group]
                # LeRobot loaders map shape [1] to a scalar column, shape [n] to a list.
                record[feature] = values[0] if len(group) == 1 else values
            record["timestamp"] = frame_index / fps
            record["frame_index"] = frame_index
            record["episode_index"] = episode_index
            record["index"] = global_index
            record["task_index"] = 0
            frame_rows.append(record)
            global_index += 1

        record = {
            "episode_index": episode_index,
            "tasks": [task],
            "length": len(sampled),
            "data/chunk_index": 0,
            "data/file_index": 0,
            "dataset_from_index": first_frame,
            "dataset_to_index": global_index,
        }
        episode_records.append(record)

    # Frame data: one shard, many episodes -- the v3 file-based layout.
    schema = pa.schema(
        [
            *[
                pa.field(
                    feature,
                    pa.float32() if len(group) == 1 else pa.list_(pa.float32()),
                )
                for feature, group in features.items()
            ],
            pa.field("timestamp", pa.float32()),
            pa.field("frame_index", pa.int64()),
            pa.field("episode_index", pa.int64()),
            pa.field("index", pa.int64()),
            pa.field("task_index", pa.int64()),
        ]
    )
    data_table = pa.Table.from_pylist(frame_rows, schema=schema)
    pq.write_table(data_table, directory / "data" / "chunk-000" / "file-000.parquet")

    # Per-feature statistics, global (meta/stats.json) and per-episode (episodes shard).
    def stats_for(table: pa.Table, feature: str) -> dict[str, list[float] | list[int]]:
        values = table.column(feature).to_pylist()
        # Scalar features (shape [1]) become one stats dimension; vectors, one per axis.
        columns = (
            [values]
            if values and not isinstance(values[0], list)
            else list(zip(*values, strict=True))
        )
        return {
            "min": [min(c) for c in columns],
            "max": [max(c) for c in columns],
            "mean": [sum(c) / len(c) for c in columns],
            "std": [(sum((v - sum(c) / len(c)) ** 2 for v in c) / len(c)) ** 0.5 for c in columns],
            "count": [len(table)],
        }

    global_stats = {feature: stats_for(data_table, feature) for feature in features}
    (directory / "meta" / "stats.json").write_text(json.dumps(global_stats, indent=2))

    for record in episode_records:
        lo, hi = record["dataset_from_index"], record["dataset_to_index"]
        episode_table = data_table.slice(lo, hi - lo)
        for feature in features:
            for key, values in stats_for(episode_table, feature).items():
                record[f"stats/{feature}/{key}"] = values
    pq.write_table(
        pa.Table.from_pylist(episode_records),
        directory / "meta" / "episodes" / "chunk-000" / "file-000.parquet",
    )

    # Task table: task_index plus the task string in pandas' index column, matching
    # what real v3 datasets on the Hub contain.
    pq.write_table(
        pa.Table.from_pylist([{"task_index": 0, "__index_level_0__": task}]),
        directory / "meta" / "tasks.parquet",
    )

    info = {
        "codebase_version": CODEBASE_VERSION,
        "robot_type": robot_type,
        "total_episodes": len(episodes),
        "total_frames": len(frame_rows),
        "total_tasks": 1,
        "chunks_size": CHUNKS_SIZE,
        "data_files_size_in_mb": DATA_FILES_SIZE_MB,
        "video_files_size_in_mb": VIDEO_FILES_SIZE_MB,
        "fps": fps,
        "splits": {"train": f"0:{len(episodes)}"},
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "video_path": None,
        "features": {
            **{
                feature: {
                    "dtype": "float32",
                    "shape": [len(group)],
                    "names": {"axes": group},
                }
                for feature, group in features.items()
            },
            "timestamp": {"dtype": "float32", "shape": [1], "names": None},
            "frame_index": {"dtype": "int64", "shape": [1], "names": None},
            "episode_index": {"dtype": "int64", "shape": [1], "names": None},
            "index": {"dtype": "int64", "shape": [1], "names": None},
            "task_index": {"dtype": "int64", "shape": [1], "names": None},
        },
    }
    (directory / "meta" / "info.json").write_text(json.dumps(info, indent=2))

    return {
        "dataset": str(directory),
        "episodes": len(episodes),
        "frames": len(frame_rows),
        "features": {feature: len(group) for feature, group in features.items()},
        "instructions": (
            f"Load with lerobot >= 0.4.0: LeRobotDataset(repo_id=..., root='{directory}') "
            "-- or push the directory to the Hugging Face Hub."
        ),
    }
