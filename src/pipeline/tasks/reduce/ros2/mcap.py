"""Back-compat alias: the MCAP reduce task is format-agnostic (`src.pipeline.tasks.reduce.mcap`)."""

from src.di import module
from src.pipeline.tasks.reduce.mcap import ReduceMcap

__all__ = ["ReduceMcap"]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ReduceMcap
