"""Artifacts created by the application."""

import base64
import hashlib
import json
import pathlib
import re
import uuid
from datetime import datetime

from settings import settings

BYTE = 1
KB = 1024 * BYTE
MB = 1024 * KB
GB = 1024 * MB


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


def checksum_sha256(file_path: str | pathlib.Path, chunk_size_bytes: int = 512 * MB) -> str:
    """Calculate the SHA-256 checksum of a local file."""
    file_hash = hashlib.sha256()

    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size_bytes):
            file_hash.update(chunk)

    digest = file_hash.digest()
    checksum_b64 = base64.b64encode(digest).decode("utf-8")

    return checksum_b64


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


def artifact_s3_key(path: pathlib.Path) -> str:
    """Return the S3 key for the given artifact path."""
    relative_path = path.relative_to(settings.ARTIFACT_DIRECTORY)
    s3_key = pathlib.Path(settings.ARTIFACT_DIRNAME) / relative_path
    return s3_key.as_posix()
