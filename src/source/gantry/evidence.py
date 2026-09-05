"""Provide a data source factory for Gantry Bench evidence bundles.

Gantry Bench (https://github.com/ShaikZaidHaaris/gantry) evaluates robot
training datasets by running them through a gauntlet of gates -- intake, data
report, a learnability probe against a shuffled control, and a robot test --
and exports each submission's evidence as a bundle: one CSV per kind of
evidence (findings, measures, per-clip signal pairs, the robot-test ladder,
the event log) plus a ``manifest.json`` that indexes the tables and carries
per-column types.

Each table becomes one Bagel topic, so questions like "which held-out clips
beat their shuffled control by the least" become auditable SQL over the
``signal_pairs`` topic rather than a re-parse of the harness's JSON.
"""

import json
import pathlib
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
from pydantic import BaseModel, ConfigDict

from src.di import module
from src.source import base, errors

MAGIC = "GANTRY_EVIDENCE"

#: Manifest column types -> Arrow types. Timestamps stay strings (ISO 8601 in
#: the bundle); the message dataset parses them to epoch seconds where a topic
#: is genuinely temporal. ``json`` columns are nested payloads serialised by
#: the exporter and are handed through as strings for DuckDB's JSON functions.
TYPE_MAP: dict[str, pa.DataType] = {
    "string": pa.string(),
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "timestamp": pa.string(),
    "json": pa.string(),
}


class EvidenceBundle(BaseModel):
    """A loaded evidence bundle: the manifest, and one Arrow table per topic."""

    manifest: dict[str, Any]
    tables: dict[str, pa.Table]

    model_config = ConfigDict(arbitrary_types_allowed=True)


class SourceFactory(base.FileBasedSourceFactory):
    """A data source factory for reading a Gantry Bench evidence bundle."""

    def __init__(self, path: str) -> None:
        """Initialize a Gantry evidence bundle data source factory.

        Args:
            path (str): Path to the unzipped evidence bundle directory.

        """
        self._manifest = self._read_manifest(path)
        super().__init__(path=path)

    @staticmethod
    def _read_manifest(path: str) -> dict[str, Any]:
        manifest_file = pathlib.Path(path) / "manifest.json"
        try:
            manifest = json.loads(manifest_file.read_text())
        except (OSError, ValueError):
            return {}
        return manifest if isinstance(manifest, dict) else {}

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the evidence bundle.

        The submission header and each gate's one-line verdict are surfaced
        here so an agent inspecting the source already knows what was judged
        and what was concluded before it queries a single row.
        """
        return {
            **self._file_based_metadata,
            "format_version": self._manifest.get("format_version"),
            "generated_at": self._manifest.get("generated_at"),
            "submission": self._manifest.get("submission", {}),
            "dataset": self._manifest.get("dataset", {}),
            "gates": self._manifest.get("gates", []),
            "tables": sorted(self._manifest.get("tables", {})),
        }

    def build(self) -> EvidenceBundle:
        """Load every table the manifest declares and return the bundle.

        A declared-but-missing or short-counted table raises rather than
        loading partially: the manifest is the bundle's word on its own
        contents, and evidence that quietly disagrees with its index is
        worse than an error.
        """
        tables: dict[str, pa.Table] = {}
        for name, spec in (self._manifest.get("tables") or {}).items():
            file = self.path / spec.get("file", f"{name}.csv")
            if not file.exists():
                raise errors.InvalidPathError(
                    f"{self.path} declares table '{name}' in its manifest "
                    f"but {file.name} is missing"
                )
            columns = spec.get("columns") or {}
            convert = pacsv.ConvertOptions(
                column_types={col: TYPE_MAP.get(kind, pa.string()) for col, kind in columns.items()}
            )
            try:
                table = pacsv.read_csv(file, convert_options=convert)
            except pa.ArrowException as exc:
                raise errors.InvalidPathError(
                    f"{file} could not be parsed as CSV: {type(exc).__name__}: {exc}"
                ) from exc

            declared = spec.get("rows")
            if declared is not None and table.num_rows != declared:
                raise errors.InvalidPathError(
                    f"{file.name} holds {table.num_rows} rows but the manifest declares {declared}"
                )
            tables[name] = table

        return EvidenceBundle(manifest=self._manifest, tables=tables)

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate if the given path is a Gantry evidence bundle directory."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_dir():
            return False, errors.PathNotDirectoryError(self.path)

        if self._manifest.get("magic") != MAGIC:
            return False, errors.InvalidPathError(
                f"{self.path} is not a Gantry evidence bundle "
                f"(manifest.json with magic '{MAGIC}' not found)."
            )

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
