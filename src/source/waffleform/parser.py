"""Parse WaffleForm YAML files into structured hardware metadata."""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml


@dataclass
class Mount:
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0


@dataclass
class Compute:
    hw: str = ""
    os: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Component:
    model: str = ""
    firmware: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Sensor:
    model: str = ""
    firmware: str | None = None
    mount: Mount | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Software:
    ros: str | None = None
    packages: dict[str, str] = field(default_factory=dict)
    pip: dict[str, str] = field(default_factory=dict)
    containers: dict[str, str] = field(default_factory=dict)


@dataclass
class WaffleForm:
    """Parsed WaffleForm representing a robot's hardware state."""

    name: str = ""
    platform: str | None = None
    compute: dict[str, Compute] = field(default_factory=dict)
    actuators: dict[str, Component] = field(default_factory=dict)
    sensors: dict[str, Sensor] = field(default_factory=dict)
    software: Software | None = None
    calibration: dict[str, str] = field(default_factory=dict)
    urdf: str | None = None

    def firmware_versions(self) -> dict[str, str]:
        """Return all firmware versions across actuators and sensors."""
        versions = {}
        for name, actuator in self.actuators.items():
            if actuator.firmware:
                versions[f"actuator.{name}"] = actuator.firmware
        for name, sensor in self.sensors.items():
            if sensor.firmware:
                versions[f"sensor.{name}"] = sensor.firmware
        return versions

    def software_packages(self) -> dict[str, str]:
        """Return all declared software packages (ros + pip + containers)."""
        pkgs: dict[str, str] = {}
        if self.software:
            for name, ver in self.software.packages.items():
                pkgs[f"ros.{name}"] = ver
            for name, ver in self.software.pip.items():
                pkgs[f"pip.{name}"] = ver
            for name, image in self.software.containers.items():
                pkgs[f"container.{name}"] = image
        return pkgs

    def to_summary(self) -> dict[str, Any]:
        """Return a concise summary for MCP tool output."""
        summary: dict[str, Any] = {
            "robot_name": self.name,
        }
        if self.platform:
            summary["platform"] = self.platform

        if self.compute:
            summary["compute"] = {
                name: {"hw": c.hw, "os": c.os} for name, c in self.compute.items()
            }

        if self.actuators:
            summary["actuators"] = {
                name: {"model": a.model, "firmware": a.firmware}
                for name, a in self.actuators.items()
            }

        if self.sensors:
            summary["sensors"] = {
                name: {"model": s.model, "firmware": s.firmware}
                for name, s in self.sensors.items()
            }

        if self.software:
            sw: dict[str, Any] = {}
            if self.software.ros:
                sw["ros"] = self.software.ros
            if self.software.packages:
                sw["packages"] = self.software.packages
            if self.software.pip:
                sw["pip"] = self.software.pip
            if self.software.containers:
                sw["containers"] = self.software.containers
            summary["software"] = sw

        if self.calibration:
            summary["calibration"] = self.calibration

        if self.urdf:
            summary["urdf"] = self.urdf

        summary["firmware_versions"] = self.firmware_versions()

        return summary


def parse_waffleform(path: str | pathlib.Path) -> WaffleForm:
    """Parse a WaffleForm YAML file into a WaffleForm dataclass.

    Args:
        path: Path to a `.waffleform.yaml` file.

    Returns:
        A parsed WaffleForm instance.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the YAML is missing the required `robot` key.
    """
    path = pathlib.Path(path)
    if not path.exists():
        raise FileNotFoundError(f"WaffleForm not found: {path}")

    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict) or "robot" not in raw:
        raise ValueError(f"Invalid WaffleForm: missing 'robot' key in {path}")

    robot = raw["robot"]

    # Parse compute
    compute = {}
    for name, data in (robot.get("compute") or {}).items():
        known = {"hw", "os"}
        compute[name] = Compute(
            hw=str(data.get("hw", "")),
            os=data.get("os"),
            extra={k: v for k, v in data.items() if k not in known},
        )

    # Parse actuators
    actuators = {}
    for name, data in (robot.get("actuators") or {}).items():
        known = {"model", "firmware"}
        actuators[name] = Component(
            model=str(data.get("model", "")),
            firmware=data.get("firmware"),
            extra={k: v for k, v in data.items() if k not in known},
        )

    # Parse sensors
    sensors = {}
    for name, data in (robot.get("sensors") or {}).items():
        mount = None
        if "mount" in data and isinstance(data["mount"], dict):
            mount = Mount(
                x=float(data["mount"].get("x", 0)),
                y=float(data["mount"].get("y", 0)),
                z=float(data["mount"].get("z", 0)),
            )
        known = {"model", "firmware", "mount"}
        sensors[name] = Sensor(
            model=str(data.get("model", "")),
            firmware=data.get("firmware"),
            mount=mount,
            extra={k: v for k, v in data.items() if k not in known},
        )

    # Parse software
    software = None
    sw_raw = robot.get("software")
    if sw_raw:
        software = Software(
            ros=sw_raw.get("ros"),
            packages={str(k): str(v) for k, v in (sw_raw.get("packages") or {}).items()},
            pip={str(k): str(v) for k, v in (sw_raw.get("pip") or {}).items()},
            containers={str(k): str(v) for k, v in (sw_raw.get("containers") or {}).items()},
        )

    # Parse calibration
    calibration = {
        str(k): str(v) for k, v in (robot.get("calibration") or {}).items()
    }

    return WaffleForm(
        name=robot.get("name", ""),
        platform=robot.get("platform"),
        compute=compute,
        actuators=actuators,
        sensors=sensors,
        software=software,
        calibration=calibration,
        urdf=robot.get("urdf"),
    )


def is_waffleform_file(path: str | pathlib.Path) -> bool:
    """Check if a path points to a WaffleForm file (.wf or .waffleform.yaml)."""
    path = pathlib.Path(path)
    return path.is_file() and (path.name.endswith(".wf") or path.name.endswith(".waffleform.yaml"))
