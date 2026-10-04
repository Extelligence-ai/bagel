"""Download and stage model bytes without executing or deserializing them."""

from __future__ import annotations

import hashlib
import io
import lzma
import stat
import struct
import tarfile
import tempfile
import zipfile
import zlib
from http import HTTPStatus
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    from src.sink.publish.applications.runtime import DockerRuntime
from urllib.parse import urlsplit

import httpx

MAX_FILES = 1000
ZIP_END_SIZE = 22
ZIP64_LOCATOR_SIZE = 20
MAX_PATH = 240
ASCII_CONTROL = 32
MAX_PATH_BYTES = 65536


def check_zip_directory(source: BinaryIO) -> None:
    """Bound directory allocation before ZipFile constructs any ZipInfo objects."""
    source.seek(0, 2)
    size = source.tell()
    source.seek(max(0, size - 65557))  # EOCD header plus maximum ZIP comment.
    tail = source.read(65557)
    offset = tail.rfind(b"PK\x05\x06")
    if offset < 0 or len(tail) - offset < ZIP_END_SIZE:
        raise ValueError("Model ZIP directory is invalid")
    _, disk, start_disk, disk_count, count, directory_size, start, comment = struct.unpack_from(
        "<4s4H2LH", tail, offset
    )
    absolute = size - len(tail) + offset
    if (
        len(tail) - offset != 22 + comment
        or disk != 0
        or start_disk != 0
        or disk_count != count
        or not 0 < count <= MAX_FILES
        or directory_size > 1024 * 1024
        or start + directory_size != absolute
        or (offset >= ZIP64_LOCATOR_SIZE and tail[offset - 20 : offset - 16] == b"PK\x06\x07")
    ):
        raise ValueError(
            "Model ZIP directory exceeds limits; use a standard ZIP with at most 1000 entries"
        )
    source.seek(0)


def safe_path(name: str) -> bool:
    """Limit archive names to ordinary relative file paths."""
    return (
        isinstance(name, str)
        and 0 < len(name) <= MAX_PATH
        and not name.startswith("/")
        and "\\" not in name
        and ":" not in name
        and all(ord(c) >= ASCII_CONTROL for c in name)
        and all(p not in ("", ".", "..") for p in name.split("/"))
    )


def download(grant: dict, model: dict, destination: BinaryIO, maximum: int) -> None:
    """Stream one short-lived HTTPS grant and verify the exact assigned checksum."""
    url = urlsplit(grant["url"])
    if url.scheme != "https" or not url.hostname or url.username or url.password:
        raise ValueError("Model download requires an authenticated HTTPS storage grant.")
    if (
        grant.get("sha256") != model["sha256"]
        or type(grant.get("size_bytes")) is not int
        or not 0 < grant["size_bytes"] <= maximum
    ):
        raise ValueError(
            "Model download exceeds the local size limit or differs from its assignment."
        )
    size, sha = 0, hashlib.sha256()
    try:
        with httpx.Client(trust_env=False, timeout=120, follow_redirects=False) as client:
            with client.stream("GET", grant["url"]) as response:
                if response.status_code != HTTPStatus.OK:
                    raise RuntimeError("Model artifact download failed; retry the rollout.")
                for chunk in response.iter_bytes(1024 * 1024):
                    size += len(chunk)
                    if size > grant["size_bytes"]:
                        raise ValueError("Model artifact exceeded its declared size.")
                    sha.update(chunk)
                    destination.write(chunk)
    except httpx.HTTPError:
        raise RuntimeError("Model artifact transfer failed; retry the rollout.") from None
    if size != grant["size_bytes"] or sha.hexdigest() != model["sha256"]:
        raise ValueError("Model artifact checksum mismatch.")
    destination.seek(0)


def make_tar(source: BinaryIO, destination: BinaryIO, model: dict, maximum: int) -> None:
    """Report invalid ZIPs without leaking transport or temporary-file details."""
    try:
        _make_tar(source, destination, model, maximum)
    except (zipfile.BadZipFile, NotImplementedError, zlib.error, lzma.LZMAError, EOFError, OSError):
        raise ValueError("Model archive is corrupt or uses unsupported ZIP compression.") from None


def _make_tar(source: BinaryIO, destination: BinaryIO, model: dict, maximum: int) -> None:
    """Create regular-file tar entries only; never extract supplied paths to the host."""
    if not safe_path(model["entrypoint"]):
        raise ValueError("Invalid model entry file.")
    with tarfile.open(fileobj=destination, mode="w") as target:
        if model["format"] == "file":
            source.seek(0, io.SEEK_END)
            size = source.tell()
            source.seek(0)
            if not 0 < size <= maximum:
                raise ValueError("Model exceeds the configured size limit.")
            member = tarfile.TarInfo(model["entrypoint"])
            member.size, member.mode = size, 0o444
            target.addfile(member, source)
        elif model["format"] == "zip":
            check_zip_directory(source)
            with zipfile.ZipFile(source) as archive:
                infos = checked_entries(archive, model["entrypoint"], maximum)
                for entry in infos:
                    if entry.is_dir():
                        continue
                    member = tarfile.TarInfo(entry.filename)
                    member.size, member.mode = entry.file_size, 0o444
                    with archive.open(entry) as reader:
                        target.addfile(member, reader)
        else:
            raise ValueError("Unsupported model artifact format.")
    destination.seek(0)


def checked_entries(
    archive: zipfile.ZipFile, entrypoint: str, maximum: int
) -> list[zipfile.ZipInfo]:
    """Reject unsafe or oversized archives before copying any member."""
    infos = archive.infolist()
    if len(infos) > MAX_FILES:
        raise ValueError("Too many model archive entries.")
    names, files, size, path_bytes = set(), set(), 0, 0
    for entry in infos:
        if entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            raise ValueError("Use ZIP with stored or DEFLATE compression for fleet models.")
        name = entry.filename.rstrip("/") if entry.is_dir() else entry.filename
        mode = stat.S_IFMT(entry.external_attr >> 16)
        if (
            not safe_path(name)
            or name in names
            or entry.flag_bits & 1
            or mode not in (0, stat.S_IFREG, stat.S_IFDIR)
        ):
            raise ValueError("Model archives require distinct safe regular files.")
        names.add(name)
        path_bytes += len(name.encode())
        if entry.is_dir():
            continue
        files.add(name)
        size += entry.file_size
        if size > maximum or path_bytes > MAX_PATH_BYTES:
            raise ValueError("Expanded model exceeds the configured size limit.")
    if entrypoint not in files or not size:
        raise ValueError("Model entry file is missing.")
    if any(str(p) in files for name in files for p in PurePosixPath(name).parents if str(p) != "."):
        raise ValueError("Conflicting model file paths.")
    return infos


def stage(runtime: DockerRuntime, model: dict, target_id: str, image: str) -> str:
    """Verify every byte before copying into a fresh, never-executed seed container."""
    from src.sink.publish.applications import contract

    if runtime.model_download is None:
        raise ValueError("Fleet model download transport is unavailable.")
    maximum = runtime.config.get("model_max_bytes", 536870912)
    grant = runtime.model_download(target_id)
    volume = (
        runtime.prefix + "-model-" + contract.digest({"model": model, "target": target_id})[:20]
    )
    seed = volume + "-seed"
    with tempfile.TemporaryFile() as source, tempfile.TemporaryFile() as archive:
        download(grant, model, source, maximum)
        make_tar(source, archive, model, maximum)
        runtime.state["staged"] = {"volume": volume, "seed": seed}
        runtime.save()  # Persist cleanup ownership before any Docker storage effect.
        # A prior interrupted staging operation owns only this target's seed.
        if runtime.inspect(seed):
            runtime.run(["rm", seed])
        runtime.run(["volume", "create", volume])
        runtime.run(
            [
                "create",
                "--name",
                seed,
                "--network",
                "none",
                "--mount",
                f"type=volume,src={volume},dst=/model,volume-nocopy",
                image,
                "/never-executed",
            ]
        )
        runtime.copy_model(archive, seed)
        runtime.run(["rm", seed])
    return volume
