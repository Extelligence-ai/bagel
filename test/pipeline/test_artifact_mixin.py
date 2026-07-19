"""Tests for the ArtifactMixin canonical artifact path."""

import pathlib

import pytest

from bagel import artifacts
from bagel.pipeline import base
from bagel.settings import settings


class _DummyTask(base.ArtifactMixin, base.Task):
    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        pass

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        pass


def _task() -> _DummyTask:
    task = _DummyTask()
    task._pipeline = "my_pipeline"
    task._name = "my_task"
    task._site = "warehouse"
    task._asset = "forklift"
    task._log_id = "log-123"
    return task


def test_artifact_path_matches_canonical_convention(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path))
    task = _task()
    path = task.artifact_path(42.5, ".csv")
    expected = artifacts.pipeline_task_artifact_path(
        "my_pipeline", "my_task", "warehouse", "forklift", "log-123", 42.5, ".csv"
    )
    assert path == expected
    assert "pipeline=my_pipeline" in str(path)
    assert "task=my_task" in str(path)
    assert path.suffix == ".csv"


def test_artifact_path_creates_parent_directory(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path))
    path = _task().artifact_path(1.0, ".gif")
    assert path.parent.is_dir()
    assert not path.exists()  # only the parent is created; the task writes the file


def test_artifact_path_without_extension_for_directory_artifacts(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path))
    path = _task().artifact_path(1.0)
    expected = artifacts.pipeline_task_artifact_path(
        "my_pipeline", "my_task", "warehouse", "forklift", "log-123", 1.0, None
    )
    assert path == expected
    assert path.name == "1.0"  # bare timestamp, no appended extension
