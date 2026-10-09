"""Back-compat alias: the MCAP reduce task is format-agnostic.

The canonical module is `bagel_mcp.pipeline.tasks.reduce.mcap`.
"""

from bagel_mcp.di import module
from bagel_mcp.pipeline.tasks.reduce.mcap import ReduceMcap

__all__ = ["ReduceMcap"]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ReduceMcap
