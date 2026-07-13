"""Tests for the cloudini CompressPointCloud task."""

import pathlib
from unittest.mock import MagicMock, patch

import pytest

from settings import settings
from src.pipeline.tasks.cloudini.compress_pointcloud import (
    CLOUDINI_CONVERTER,
    CompressPointCloud,
    _converter_available,
)


def _task(tmp_path: pathlib.Path, cloudini: bool = True) -> CompressPointCloud:
    """Build a task with the operator attributes an Operator.build would set."""
    task = CompressPointCloud(cloudini=cloudini)
    task._pipeline = "compress_lidar"
    task._name = "compress_point_cloud"
    task._site = "warehouse"
    task._asset = "forklift"
    task._log_id = "log123"
    task._path = str(tmp_path / "input.mcap")
    return task


def test_opt_out_via_flag_skips(tmp_path: pathlib.Path) -> None:
    task = _task(tmp_path, cloudini=False)
    with patch("subprocess.run") as run:
        assert task.execute(asof_seconds=1.0, lookback=None) is None
        run.assert_not_called()


@patch("shutil.which", return_value=None)
def test_skips_when_binary_missing(_which: MagicMock, tmp_path: pathlib.Path) -> None:
    task = _task(tmp_path)
    with patch("subprocess.run") as run:
        assert task.execute(asof_seconds=1.0, lookback=None) is None
        run.assert_not_called()


@patch("shutil.which", return_value="/usr/local/bin/cloudini_rosbag_converter")
def test_skips_when_globally_disabled(
    _which: MagicMock, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", False)
    with patch("subprocess.run") as run:
        assert _task(tmp_path).execute(asof_seconds=1.0, lookback=None) is None
        run.assert_not_called()


@patch("shutil.which", return_value="/usr/local/bin/cloudini_rosbag_converter")
def test_builds_correct_compress_command(
    _which: MagicMock, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    task = _task(tmp_path)

    with patch("subprocess.run") as run:
        run.return_value = MagicMock(args=[], stdout="")
        result = task.execute(asof_seconds=1.0, lookback=None)

    assert run.call_count == 1
    command = run.call_args.args[0]
    assert command[0] == CLOUDINI_CONVERTER
    assert command[1:3] == ["-f", task.path]
    assert command[3] == "-o"
    assert command[-1] == "-c"  # compression mode
    assert command[4].endswith(".mcap")
    assert result == [pathlib.Path(command[4])]


def test_converter_available_true_when_enabled_and_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    with patch("shutil.which", return_value="/usr/local/bin/cloudini_rosbag_converter"):
        assert _converter_available() is True
