"""Tests for WaffleForm parser."""

import tempfile
from pathlib import Path

import pytest

from src.source.waffleform.parser import WaffleForm, is_waffleform_file, parse_waffleform


SAMPLE_WAFFLEFORM = """\
robot:
  name: warehouse-amr-07
  platform: clearpath-jackal

  compute:
    primary:
      hw: nvidia-jetson-orin
      jetpack: "6.0"
      os: ubuntu-22.04

  actuators:
    arm:
      model: ur5e
      firmware: "5.12.0"
    gripper:
      model: robotiq-2f85
      firmware: "4.0.1"

  sensors:
    lidar:
      model: rplidar-a2
      firmware: "1.29"
      mount: {x: 0, y: 0, z: 0.3}
    camera:
      model: realsense-d435i
      firmware: "5.14.0"

  software:
    ros: humble
    packages:
      nav2: "1.1.12"
      moveit2: "2.7.4"
    pip:
      ultralytics: "8.1.0"
    containers:
      perception: "ghcr.io/myteam/perception:v2.3"

  calibration:
    hand_eye: cal/hand_eye_v3.yaml
    camera: cal/d435i_intrinsics.yaml

  urdf: robot.urdf.xacro
"""


@pytest.fixture
def waffleform_path(tmp_path: Path) -> Path:
    """Write sample WaffleForm to a temp file."""
    path = tmp_path / "robot.waffleform.yaml"
    path.write_text(SAMPLE_WAFFLEFORM)
    return path


def test_is_waffleform_file(waffleform_path: Path, tmp_path: Path) -> None:
    assert is_waffleform_file(waffleform_path) is True
    assert is_waffleform_file(tmp_path / "foo.bag") is False
    assert is_waffleform_file(tmp_path / "nonexistent.waffleform.yaml") is False


def test_parse_basic(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)

    assert form.name == "warehouse-amr-07"
    assert form.platform == "clearpath-jackal"
    assert form.urdf == "robot.urdf.xacro"


def test_parse_compute(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)

    assert "primary" in form.compute
    assert form.compute["primary"].hw == "nvidia-jetson-orin"
    assert form.compute["primary"].os == "ubuntu-22.04"
    assert form.compute["primary"].extra["jetpack"] == "6.0"


def test_parse_actuators(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)

    assert len(form.actuators) == 2
    assert form.actuators["arm"].model == "ur5e"
    assert form.actuators["arm"].firmware == "5.12.0"
    assert form.actuators["gripper"].model == "robotiq-2f85"


def test_parse_sensors(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)

    assert len(form.sensors) == 2
    assert form.sensors["lidar"].model == "rplidar-a2"
    assert form.sensors["lidar"].mount is not None
    assert form.sensors["lidar"].mount.z == 0.3
    assert form.sensors["camera"].firmware == "5.14.0"


def test_parse_software(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)

    assert form.software is not None
    assert form.software.ros == "humble"
    assert form.software.packages["nav2"] == "1.1.12"
    assert form.software.pip["ultralytics"] == "8.1.0"
    assert form.software.containers["perception"] == "ghcr.io/myteam/perception:v2.3"


def test_parse_calibration(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)

    assert form.calibration["hand_eye"] == "cal/hand_eye_v3.yaml"
    assert form.calibration["camera"] == "cal/d435i_intrinsics.yaml"


def test_firmware_versions(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)
    fw = form.firmware_versions()

    assert fw["actuator.arm"] == "5.12.0"
    assert fw["actuator.gripper"] == "4.0.1"
    assert fw["sensor.lidar"] == "1.29"
    assert fw["sensor.camera"] == "5.14.0"


def test_software_packages(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)
    pkgs = form.software_packages()

    assert pkgs["ros.nav2"] == "1.1.12"
    assert pkgs["pip.ultralytics"] == "8.1.0"
    assert pkgs["container.perception"] == "ghcr.io/myteam/perception:v2.3"


def test_to_summary(waffleform_path: Path) -> None:
    form = parse_waffleform(waffleform_path)
    summary = form.to_summary()

    assert summary["robot_name"] == "warehouse-amr-07"
    assert summary["platform"] == "clearpath-jackal"
    assert "primary" in summary["compute"]
    assert "arm" in summary["actuators"]
    assert "lidar" in summary["sensors"]
    assert summary["software"]["ros"] == "humble"
    assert "firmware_versions" in summary


def test_invalid_file(tmp_path: Path) -> None:
    bad = tmp_path / "bad.waffleform.yaml"
    bad.write_text("not_robot: foo")
    with pytest.raises(ValueError, match="missing 'robot' key"):
        parse_waffleform(bad)


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        parse_waffleform(tmp_path / "nope.waffleform.yaml")


def test_minimal_waffleform(tmp_path: Path) -> None:
    """Minimal valid WaffleForm — just a name."""
    path = tmp_path / "minimal.waffleform.yaml"
    path.write_text("robot:\n  name: bare-bot\n")
    form = parse_waffleform(path)

    assert form.name == "bare-bot"
    assert form.compute == {}
    assert form.actuators == {}
    assert form.sensors == {}
    assert form.software is None
    assert form.calibration == {}
