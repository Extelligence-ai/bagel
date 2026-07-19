"""Run one pipeline configuration across many data sources (batch mode).

Given a base pipeline config and a set of paths (explicit paths or glob patterns), run
the same pipeline against each source, overriding only `path`. Each source gets its own
artifact directory (the log id is derived from site, asset, and path), and one failing
source does not abort the batch -- failures are reported per path.
"""

import glob
import logging
from typing import Any

from bagel.pipeline import base


def expand_paths(patterns: list[str]) -> list[str]:
    """Expand glob patterns into concrete paths, preserving order and de-duplicating.

    A pattern that matches nothing is kept as-is (it may be an explicit path or a
    directory), so callers can mix globs and literal paths freely.

    Args:
        patterns: Paths or glob patterns (e.g. ["logs/*", "one_more_bag"]).

    Returns:
        The expanded, de-duplicated list of paths in first-seen order.

    """
    paths: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        matches = sorted(glob.glob(pattern)) or [pattern]
        for match in matches:
            if match not in seen:
                seen.add(match)
                paths.append(match)
    return paths


def run_batch(config: dict[str, Any], paths: list[str]) -> list[dict[str, Any]]:
    """Run a pipeline config against each path, returning a per-path result.

    Args:
        config: The base pipeline config. Its `path` is overridden per source.
        paths: The already-expanded data source paths.

    Returns:
        One dict per path with `path`, `status` ("completed" or "failed"), and either
        `artifacts` (on success) or `error` (on failure).

    """
    results: list[dict[str, Any]] = []
    for path in paths:
        source_config = {**config, "path": path}
        try:
            pipeline = base.Pipeline.build(source_config)
            produced = pipeline.run_all()
            results.append(
                {
                    "path": path,
                    "status": "completed",
                    "artifacts": [str(artifact) for artifact in produced],
                }
            )
        except Exception as error:  # one bad source must not abort the batch
            logging.error("Batch pipeline failed for '%s': %s", path, error)
            results.append({"path": path, "status": "failed", "error": str(error)})
    return results


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize batch results: source counts and total artifacts produced."""
    completed = [result for result in results if result["status"] == "completed"]
    failed = [result for result in results if result["status"] == "failed"]
    return {
        "sources": len(results),
        "completed": len(completed),
        "failed": len(failed),
        "artifacts": sum(len(result.get("artifacts", [])) for result in completed),
        "results": results,
    }
