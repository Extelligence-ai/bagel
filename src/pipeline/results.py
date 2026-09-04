"""Structured pipeline execution outcomes, including permitted failures."""

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class RunSummary:
    """Count completed, skipped, and failed cadence runs."""

    succeeded: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def status(self) -> Literal["completed", "partial", "failed"]:
        """Distinguish successful completion from tolerated failures."""
        if self.failed:
            return "partial" if self.succeeded else "failed"
        return "completed"
