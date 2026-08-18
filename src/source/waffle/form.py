"""Provide a data source for WaffleForm files (hardware as code). EXPERIMENTAL BETA.

A `robot.waffleform.yaml` declares a robot's full hardware state -- compute,
actuators, sensors, firmware, calibration, software, URDF -- as managed by
`waffle-iron` (https://github.com/arunvenkatadri/waffle-iron). Bagel maps each
component category to a topic and each component to a row, so hardware state is
SQL-queryable like any other source: ``SELECT * FROM sensors WHERE
sensors['firmware'] < '5.14'``.

A WaffleForm is a snapshot, not a time series: every row carries the file's
modification time as its timestamp. Accumulate snapshots (e.g. via a periodic
``waffle snap`` pipeline) and hardware state becomes history.
"""

import pathlib
import uuid
from typing import Any

import yaml

from src.di import module
from src.source import base, errors

# Categories whose values are name -> component mappings.
COMPONENT_CATEGORIES = ("compute", "actuators", "sensors", "calibration")


def _flatten(value: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten nested component fields into dotted keys (mount: {x: 0} -> mount.x)."""
    out: dict[str, Any] = {}
    for key, item in value.items():
        name = f"{prefix}{key}"
        if isinstance(item, dict):
            out.update(_flatten(item, prefix=f"{name}."))
        else:
            out[name] = item
    return out


class WaffleForm:
    """A parsed WaffleForm: component categories as topics, components as rows."""

    def __init__(self, path: str, snap_seconds: float) -> None:
        """Parse the WaffleForm at `path`; rows are stamped with `snap_seconds`."""
        self.snap_seconds = snap_seconds
        content = yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8"))
        robot = content.get("robot") or {}

        raw_rows = self._component_rows(robot)

        # Per topic: infer the union schema, then coerce values to it.
        self.fields: dict[str, dict[str, str]] = {}
        self.rows: dict[str, list[dict[str, Any]]] = {}
        for topic, entries in raw_rows.items():
            keys: dict[str, str] = {}
            for entry in entries:
                for key, value in entry.items():
                    numeric = isinstance(value, int | float) and not isinstance(value, bool)
                    if keys.get(key, "float") != "float" or not numeric:
                        keys[key] = "string"
                    else:
                        keys[key] = "float"
            self.fields[topic] = keys
            self.rows[topic] = [
                {
                    key: (
                        None
                        if entry.get(key) is None
                        else float(entry[key])
                        if kind == "float"
                        else str(entry[key])
                    )
                    for key, kind in keys.items()
                }
                for entry in entries
            ]

    @staticmethod
    def _component_rows(robot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        """Normalize the form into rows per category."""
        rows: dict[str, list[dict[str, Any]]] = {}
        for category in COMPONENT_CATEGORIES:
            components = robot.get(category)
            if not isinstance(components, dict):
                continue
            rows[category] = [
                {
                    "name": name,
                    **(_flatten(spec) if isinstance(spec, dict) else {"reference": spec}),
                }
                for name, spec in components.items()
            ]

        software = robot.get("software")
        if isinstance(software, dict):
            entries = []
            for name, value in software.items():
                if isinstance(value, dict):  # packages: {nav2: "1.1.12", ...}
                    entries.extend({"name": p, "version": str(v)} for p, v in value.items())
                else:
                    entries.append({"name": name, "version": str(value)})
            rows["software"] = entries

        summary = {k: v for k, v in robot.items() if not isinstance(v, dict)}
        if summary:
            rows["robot"] = [summary]
        return rows


class SourceFactory(base.FileBasedSourceFactory):
    """A data source factory for WaffleForm files."""

    def __init__(self, path: str) -> None:
        """Initialize the WaffleForm data source factory.

        Args:
            path (str): Path to the `.waffleform.yaml` file.

        """
        super().__init__(path)
        self._form = WaffleForm(path, snap_seconds=self.path.stat().st_mtime)

    @property
    def uuid(self) -> str:
        """Identify the snapshot by content and snap time.

        The content hash alone is not enough: rows are stamped with the snap
        time, so the same form re-snapped later is new data and must not hit
        the previous snapshot's cached Arrow file.
        """
        return str(uuid.uuid5(uuid.NAMESPACE_OID, f"{super().uuid}_{self._form.snap_seconds}"))

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the hardware declaration."""
        robot = self._form.rows.get("robot", [{}])[0]
        return {
            **self._file_based_metadata,
            "snap_seconds": self._form.snap_seconds,
            "robot": robot,
            "categories": {topic: len(rows) for topic, rows in self._form.rows.items()},
        }

    def build(self) -> WaffleForm:
        """Return the parsed WaffleForm."""
        return self._form

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate that the path is a WaffleForm file."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)
        if not self.path.name.endswith(".waffleform.yaml"):
            return False, errors.InvalidFileExtensionError(".waffleform.yaml", self.path)
        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
