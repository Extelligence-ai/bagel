"""Write the annotations of the gates that let this run through, as a JSON artifact.

The file holds ``asof_seconds`` plus one object per annotating gate, keyed by the gate's
name (``{"asof_seconds": ..., "anomaly": {...}}``). Pair it with a snippet task: both
name their artifact after the same timestamp, e.g. ``task=snip_mcap/.../1700000410.0.mcap``
and ``task=write_annotations/.../1700000410.0.json``, so an upload task sends each log
slice together with its label.
"""

import json
import pathlib

from src.di import module
from src.pipeline import base


class WriteAnnotations(base.ArtifactMixin, base.Task):
    """Write the gates' annotations for this run as JSON; nothing if there are none."""

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Implement `base.Operator.setup`; no data source access is needed."""

    def execute(
        self, asof_seconds: float, lookback: base.Lookback | None
    ) -> list[pathlib.Path] | None:
        """Implement `base.Task.execute`."""
        if not self.gate_annotations:
            return None
        path = self.artifact_path(asof_seconds, ".json")
        record = {"asof_seconds": asof_seconds, **self.gate_annotations}
        path.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
        return [path]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = WriteAnnotations
