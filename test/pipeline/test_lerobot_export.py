"""Tests for the LeRobotDataset v3.0 export, validated against the published layout."""

import json
import pathlib

import duckdb
import pytest

import server
from settings import settings

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"
SAMPLE_ARGS = {"timestamp_column": "t", "timestamp_format": "seconds"}
FEATURES = {
    "observation.state": ["message/vel", "message/t"],  # shape [2]: a list column
    "action": ["message/accel_x"],  # shape [1]: a scalar column, per the LeRobot loader
}


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def _export(**overrides: object) -> dict:
    params = {
        "path": SAMPLE,
        "topics": ["message"],
        "episodes": [
            {"start_seconds": 5.0, "end_seconds": 10.0},
            {"start_seconds": 20.0, "end_seconds": 30.0},
        ],
        "features": FEATURES,
        "fps": 2,
        "task": "drive without hard braking",
        "name": "brake study",
        "robot_type": "test_vehicle",
        "args": SAMPLE_ARGS,
    }
    params.update(overrides)
    return server.export_for_lerobot(**params)


def test_v3_directory_layout_and_info(tmp_path: pathlib.Path) -> None:
    result = _export()
    root = pathlib.Path(result["dataset"])

    assert (root / "data" / "chunk-000" / "file-000.parquet").exists()
    assert (root / "meta" / "episodes" / "chunk-000" / "file-000.parquet").exists()
    assert (root / "meta" / "tasks.parquet").exists()
    assert (root / "meta" / "stats.json").exists()

    info = json.loads((root / "meta" / "info.json").read_text())
    assert info["codebase_version"] == "v3.0"
    assert info["fps"] == 2
    assert info["robot_type"] == "test_vehicle"
    assert info["total_episodes"] == 2
    assert info["splits"] == {"train": "0:2"}
    assert info["data_path"] == "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
    assert info["features"]["observation.state"]["shape"] == [2]
    assert info["features"]["observation.state"]["names"] == {"axes": ["message/vel", "message/t"]}
    assert info["features"]["action"]["shape"] == [1]
    assert info["features"]["action"]["dtype"] == "float32"
    # 5s window at 2 fps = 11 frames; 10s window = 21 frames.
    assert result["frames"] == info["total_frames"] == 32


def test_frame_table_matches_hub_schema() -> None:
    result = _export()
    data_file = pathlib.Path(result["dataset"]) / "data" / "chunk-000" / "file-000.parquet"
    table = duckdb.sql(f"SELECT * FROM read_parquet('{data_file}')")

    assert set(table.columns) == {
        "observation.state",
        "action",
        "timestamp",
        "frame_index",
        "episode_index",
        "index",
        "task_index",
    }

    rows = duckdb.sql(
        f"SELECT episode_index, COUNT(*), MIN(frame_index), MAX(frame_index), "
        f"MIN(timestamp), MAX(timestamp) FROM read_parquet('{data_file}') GROUP BY 1 ORDER BY 1"
    ).fetchall()
    # Per-episode: frame_index restarts at 0 and timestamps are relative at 1/fps.
    assert rows[0] == (0, 11, 0, 10, 0.0, 5.0)
    assert rows[1] == (1, 21, 0, 20, 0.0, 10.0)

    # Scalar-vs-list column mapping, matching how LeRobot loaders read shapes.
    types = dict(zip(table.columns, [str(t) for t in table.types], strict=True))
    assert types["action"] == "FLOAT"
    assert types["observation.state"] == "FLOAT[]"

    # The global index is contiguous across episodes.
    lo, hi = duckdb.sql(
        f'SELECT MIN("index"), MAX("index") FROM read_parquet(\'{data_file}\')'
    ).fetchone()
    assert (lo, hi) == (0, 31)


def test_episode_metadata_offsets_and_stats() -> None:
    result = _export()
    episodes_file = (
        pathlib.Path(result["dataset"]) / "meta" / "episodes" / "chunk-000" / "file-000.parquet"
    )
    rows = duckdb.sql(
        f"SELECT episode_index, tasks, length, dataset_from_index, dataset_to_index, "
        f"\"stats/observation.state/count\" FROM read_parquet('{episodes_file}') ORDER BY 1"
    ).fetchall()

    assert rows[0][:5] == (0, ["drive without hard braking"], 11, 0, 11)
    assert rows[1][:5] == (1, ["drive without hard braking"], 21, 11, 32)
    assert rows[1][5] == [21]

    stats = json.loads((pathlib.Path(result["dataset"]) / "meta" / "stats.json").read_text())
    assert stats["observation.state"]["count"] == [32]
    assert stats["action"]["min"][0] <= stats["action"]["max"][0]


def test_rejects_unknown_signals_and_empty_windows() -> None:
    with pytest.raises(ValueError, match="Unknown or non-numeric"):
        _export(features={"observation.state": ["message/no_such"]})

    with pytest.raises(ValueError, match="required"):
        _export(episodes=[])
