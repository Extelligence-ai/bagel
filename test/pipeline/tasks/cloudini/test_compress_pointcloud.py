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

    def write_output(command: list[str], **kwargs: object) -> MagicMock:
        pathlib.Path(command[command.index("-o") + 1]).write_bytes(b"mcap")
        return MagicMock(args=command, stdout="")

    with patch("subprocess.run", side_effect=write_output) as run:
        result = task.execute(asof_seconds=1.0, lookback=None)

    assert run.call_count == 1
    command = run.call_args.args[0]
    assert command[0] == CLOUDINI_CONVERTER
    assert command[1:3] == ["-f", task.path]
    assert command[3] == "-o"
    assert command[-2:] == ["-c", "-y"]  # compression mode, non-interactive overwrite
    assert command[4].endswith(".mcap")
    assert result == [pathlib.Path(command[4])]


def test_converter_available_true_when_enabled_and_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    with patch("shutil.which", return_value="/usr/local/bin/cloudini_rosbag_converter"):
        assert _converter_available() is True


@patch("shutil.which", return_value="/usr/bin/mcap_converter")
def test_command_passes_yes_flag_for_deterministic_retries(
    _which: MagicMock, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copilot on #142: artifact_path is deterministic, so a retry finds the
    previous output in place; without -y the converter prompts (and blocks a
    non-interactive pipeline) instead of overwriting."""
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    task = _task(tmp_path)

    def write_output(command: list[str], **kwargs: object) -> MagicMock:
        pathlib.Path(command[command.index("-o") + 1]).write_bytes(b"mcap")
        return MagicMock(args=command, stdout="")

    with patch("subprocess.run", side_effect=write_output) as run:
        task.execute(asof_seconds=1.0, lookback=None)

    assert "-y" in run.call_args.args[0]


@patch("shutil.which", return_value="/usr/bin/mcap_converter")
def test_missing_output_returns_no_artifact(
    _which: MagicMock, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copilot on #142: the converter exits 0 without writing anything when
    the bag has no pointcloud topics; the task must not report a nonexistent
    artifact (uploads would raise FileNotFoundError)."""
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    task = _task(tmp_path)

    with patch("subprocess.run") as run:
        run.return_value = MagicMock(args=[], stdout="")
        result = task.execute(asof_seconds=1.0, lookback=None)

    assert result is None


@patch("shutil.which", return_value="/usr/bin/mcap_converter")
def test_directory_source_resolves_inner_mcap(
    _which: MagicMock, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex on #142: rosbag2 MCAP directories are a supported source layout;
    the converter must receive the contained .mcap file, not the directory."""
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    bag_dir = tmp_path / "rosbag2_dir"
    bag_dir.mkdir()
    inner = bag_dir / "rosbag2_0.mcap"
    inner.write_bytes(b"mcap")
    task = _task(tmp_path)
    task._path = str(bag_dir)

    def write_output(command: list[str], **kwargs: object) -> MagicMock:
        pathlib.Path(command[command.index("-o") + 1]).write_bytes(b"mcap")
        return MagicMock(args=command, stdout="")

    with patch("subprocess.run", side_effect=write_output) as run:
        result = task.execute(asof_seconds=1.0, lookback=None)

    command = run.call_args.args[0]
    assert command[command.index("-f") + 1] == str(inner)
    assert result is not None and len(result) == 1


@patch("shutil.which", return_value="/usr/bin/mcap_converter")
def test_multi_part_directory_compresses_every_segment(
    _which: MagicMock, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    bag_dir = tmp_path / "rosbag2_dir"
    bag_dir.mkdir()
    (bag_dir / "rosbag2_0.mcap").write_bytes(b"mcap")
    (bag_dir / "rosbag2_1.mcap").write_bytes(b"mcap")
    task = _task(tmp_path)
    task._path = str(bag_dir)

    def write_output(command: list[str], **kwargs: object) -> MagicMock:
        pathlib.Path(command[command.index("-o") + 1]).write_bytes(b"mcap")
        return MagicMock(args=command, stdout="")

    with patch("subprocess.run", side_effect=write_output) as run:
        result = task.execute(asof_seconds=1.0, lookback=None)

    assert run.call_count == 2
    assert result is not None and len(result) == 2
    assert len({str(artifact) for artifact in result}) == 2


@patch("shutil.which", return_value="/usr/bin/mcap_converter")
def test_stale_artifact_from_prior_run_is_not_reported(
    _which: MagicMock, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copilot (suppressed) on #142: with a deterministic output path, a retry
    where the converter writes nothing must not report the previous run's
    artifact as this run's result."""
    monkeypatch.setattr(settings, "CLOUDINI_ENABLED", True)
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    task = _task(tmp_path)

    stale = task.artifact_path(1.0, ".mcap")
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"stale from an earlier run")

    with patch("subprocess.run") as run:
        run.return_value = MagicMock(args=[], stdout="")
        result = task.execute(asof_seconds=1.0, lookback=None)

    assert result is None
