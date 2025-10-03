"""Artifacts created by the application."""

import hashlib
import pathlib

from settings import settings


def short_digest(seeds: list[str]) -> str:
    """Generate a short SHA-256 digest from a list of seeds."""
    if not seeds:
        raise ValueError("Seeds list must not be empty.")
    return hashlib.sha256("_".join(seeds).encode("utf8")).hexdigest()[:8]


def arrow_file(source_uuid: str, seeds: list[str], prefix: str) -> pathlib.Path:
    """Generate an Apache Arrow file path for caching purposes."""
    stem = f"{prefix}_{short_digest(seeds)}"
    return (
        pathlib.Path(settings.CACHE_DIRECTORY)
        / "data"
        / f"source_id={source_uuid}"
        / f"{stem}.arrow"
    )


def sink_directory(sink_uuid: str) -> pathlib.Path:
    """Generate a directory path for storing data from a topic message sink."""
    return pathlib.Path(settings.CACHE_DIRECTORY) / "data" / f"sink={sink_uuid}"


def git_clone_directory() -> pathlib.Path:
    """Generate a directory path for cloning git repositories."""
    return pathlib.Path(settings.CACHE_DIRECTORY) / "repos"
