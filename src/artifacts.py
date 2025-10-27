"""Artifacts created by the application."""

import hashlib
import json
import pathlib
import re
import uuid
from datetime import datetime

from settings import settings


def is_lower_snake_case(s: str) -> bool:
    """Return True if the string is in lower_snake_case format."""
    pattern = r"^[a-z0-9]+(?:_[a-z0-9]+)*$"
    return bool(re.fullmatch(pattern, s))


def to_lower_snake_case(name: str) -> str:
    """Convert a PascalCase or camelCase string to lower_snake_case."""
    s1 = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)  # handle transitions
    s2 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s1)  # handle acronyms
    return s2.lower()


def short_digest(seeds: list[str]) -> str:
    """Generate a short SHA-256 digest from a list of seeds."""
    if not seeds:
        raise ValueError("Seeds list must not be empty.")
    return hashlib.sha256("_".join(seeds).encode("utf8")).hexdigest()[:8]


#############################
### Cached artifact paths ###
#############################


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
    """Generate a directory path for storing data from a topic sink."""
    return pathlib.Path(settings.CACHE_DIRECTORY) / "data" / f"sink={sink_uuid}"


def git_clone_directory() -> pathlib.Path:
    """Generate a directory path for cloning git repositories."""
    return pathlib.Path(settings.CACHE_DIRECTORY) / "repos"


######################
### Artifact paths ###
######################


def generate_log_uuid(site: str, asset: str, path: str) -> str:
    """Return a UUID based on site, asset, and path."""
    seeds = [site, asset, path]
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, json.dumps(seeds)))


def pipeline_task_artifact_path(  # noqa: PLR0913
    pipeline: str,
    task: str,
    site: str,
    asset: str,
    log_id: str,
    timestamp_seconds: float,
    extension: str | None,
) -> pathlib.Path:
    """Return the artifact path for a pipeline task at a given timestamp."""
    datestr = datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d")
    parent = (
        pathlib.Path(settings.ARTIFACT_DIRECTORY)
        / f"pipeline={pipeline}"
        / f"task={task}"
        / f"datestr={datestr}"
        / f"site={site}"
        / f"asset={asset}"
        / f"log_id={log_id}"
    )
    if extension is not None:
        return parent / f"{timestamp_seconds}.{extension.lstrip('.')}"
    else:
        return parent / f"{timestamp_seconds}"
