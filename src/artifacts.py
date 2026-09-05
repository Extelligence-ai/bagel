"""Artifacts created by the application."""

import base64
import hashlib
import json
import logging
import pathlib
import re
import uuid
from datetime import datetime

import filelock

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


def cached_arrow_files() -> list[pathlib.Path]:
    """All cached .arrow query results (never sink buffers, repos, or artifacts)."""
    data_directory = pathlib.Path(settings.CACHE_DIRECTORY) / "data"
    return [file for file in data_directory.glob("source_id=*/**/*.arrow") if file.is_file()]


def evict_arrow_cache() -> int:
    """Delete oldest-by-access cached .arrow files until under CACHE_MAX_BYTES.

    Cache entries are derived data keyed by (source, topics, window, ffill) and
    rebuild on demand, so deletion is always safe. Called before each new cache
    write; the incoming file may overshoot the cap until the next write evicts.
    Returns the number of files deleted; no-op when CACHE_MAX_BYTES is 0.

    The inventory-and-delete pass runs under one cache-wide, nonblocking lock so
    two concurrent callers never each compute a total against a stale snapshot
    and delete more entries between them than the cap requires; a caller that
    loses the race backs off and returns 0, leaving eviction to the pass already
    running. Per-entry locks are still used for the actual deletes so eviction
    keeps skipping entries a reader or writer currently holds.
    """
    limit_bytes = settings.CACHE_MAX_BYTES
    if not limit_bytes:
        return 0
    data_directory = pathlib.Path(settings.CACHE_DIRECTORY) / "data"
    data_directory.mkdir(parents=True, exist_ok=True)
    try:
        with filelock.FileLock(str(data_directory / ".eviction.lock"), timeout=0):
            deleted = _evict_arrow_cache_locked(limit_bytes)
    except filelock.Timeout:
        return 0  # another eviction pass is already in flight
    if deleted:
        logging.warning(
            "Evicted %d cached arrow file(s) to keep the query cache under %d bytes "
            "(CACHE_MAX_BYTES; 0 disables eviction)",
            deleted,
            limit_bytes,
        )
    return deleted


def _evict_arrow_cache_locked(limit_bytes: int) -> int:
    """Delete oldest-by-access cached files until under `limit_bytes`.

    Callers must hold the cache-wide eviction lock; see `evict_arrow_cache`.
    """
    entries = []
    for file in cached_arrow_files():
        try:
            stat = file.stat()
        except OSError:
            continue  # deleted by a concurrent process; nothing to account
        entries.append((max(stat.st_atime, stat.st_mtime), stat.st_size, file))
    total = sum(size for _, size, _ in entries)
    deleted = 0
    for _, size, file in sorted(entries):
        if total <= limit_bytes:
            break
        try:
            with filelock.FileLock(str(file) + ".lock", timeout=0):
                file.unlink(missing_ok=True)
        except filelock.Timeout:
            continue
        total -= size
        deleted += 1
    return deleted


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


def directory_size_bytes(directory: str | pathlib.Path) -> int:
    """Total size of all files under a directory; 0 if it does not exist."""
    root = pathlib.Path(directory)
    if not root.exists():
        return 0
    total = 0
    for file in root.glob("**/*"):
        if not file.is_file():
            continue
        try:
            total += file.stat().st_size
        except OSError:
            continue  # deleted between glob and stat; nothing to account
    return total
