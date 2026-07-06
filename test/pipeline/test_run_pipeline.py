"""Tests for the run_pipeline and save_pipeline MCP tools.

Uses the pure-Python PyArrow CSV source and the write_topics_to_file task, so the
build -> run -> artifact path executes without ROS.
"""

import pathlib

import pytest
import yaml

import server
from settings import settings

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"


def _config() -> dict:
    return {
        "name": "csv_smoke",
        "site": "test_site",
        "asset": "test_asset",
        "path": SAMPLE,
        "allow_failure": False,
        "cadence": {"topic": "message", "when": "once_at_end"},
        "tasks": [
            {
                "module": "src.pipeline.tasks.write_topics_to_file",
                "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
                "args": {"topics": ["message"], "output_format": "csv"},
            }
        ],
    }


def test_save_pipeline_writes_loadable_yaml(tmp_path: pathlib.Path) -> None:
    output = server.save_pipeline(_config(), "csv_smoke", directory=str(tmp_path))
    written = pathlib.Path(output)
    assert written.exists()
    loaded = yaml.safe_load(written.read_text())
    assert loaded["name"] == "csv_smoke"
    assert loaded["cadence"]["when"] == "once_at_end"
    # Order is preserved (sort_keys=False) so the file reads like an authored template.
    assert list(loaded) == list(_config())


def test_save_pipeline_rejects_non_snake_case_name(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="lower_snake_case"):
        server.save_pipeline(_config(), "Bad Name", directory=str(tmp_path))


def test_run_pipeline_produces_artifact(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path))
    result = server.run_pipeline(_config())
    assert result["status"] == "completed"
    assert result["pipeline"] == "csv_smoke"
    assert len(result["artifacts"]) == 1
    artifact = pathlib.Path(result["artifacts"][0])
    assert artifact.exists()
    assert artifact.suffix == ".csv"
