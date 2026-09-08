"""Tests for the waffle snapshot task and tool, against a fake waffle binary."""

import pathlib
import stat

import pytest

import server
from settings import settings
from src.di.types.data_source import DataSource, resolve
from src.pipeline.tasks.waffle.snap import WaffleSnap, run_waffle

FORM = """robot:
  name: fake-bot
  platform: test-rig
  sensors:
    camera:
      model: realsense-d435i
      firmware: "5.14.0"
"""


@pytest.fixture()
def fake_waffle(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """A fake `waffle` on PATH: `init`/`snap` write a fixture WaffleForm to cwd."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "waffle"
    script.write_text(
        f"#!/bin/sh\necho \"scanned 2 devices\"\ncat > robot.waffleform.yaml <<'EOF'\n{FORM}EOF\n"
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin:/bin")
    workdir = tmp_path / "robot"
    workdir.mkdir()
    return workdir


def test_run_waffle_inits_then_snaps(fake_waffle: pathlib.Path) -> None:
    form = run_waffle(str(fake_waffle))

    assert form.name == "robot.waffleform.yaml"
    assert form.exists()
    assert resolve(str(form)) is DataSource.WAFFLE_FORM


def test_missing_binary_fails_with_install_hint(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(RuntimeError, match="cargo install waffle-iron"):
        run_waffle(str(tmp_path))


def test_task_archives_timestamped_queryable_snapshots(
    fake_waffle: pathlib.Path, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    task = WaffleSnap(workdir=str(fake_waffle))
    task._name = "snap"
    task._pipeline = "hardware_history"
    task._site = "warehouse"
    task._asset = "amr_07"
    task._path = str(fake_waffle)
    task._log_id = "log"
    task._upload = False

    produced = task.execute(asof_seconds=1662400000.0)

    assert len(produced) == 1
    artifact = produced[0]
    assert artifact.name == "1662400000.waffleform.yaml"
    assert "waffle/hardware_history" in str(artifact)
    # The archived snapshot is itself a queryable Bagel data source.
    assert resolve(str(artifact)) is DataSource.WAFFLE_FORM


def test_snap_hardware_tool_returns_summary(fake_waffle: pathlib.Path) -> None:
    result = server.snap_hardware(directory=str(fake_waffle))

    assert result["form"].endswith("robot.waffleform.yaml")
    assert result["robot"]["name"] == "fake-bot"
    assert result["categories"] == {"sensors": 1, "robot": 1}
