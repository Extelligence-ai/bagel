"""Artifact transfer, integrity and safe archive conversion without model execution."""

import hashlib
import io
import stat
import tarfile
import zipfile
from pathlib import Path

import httpx
import pytest

from src.sink.publish.applications import artifacts


def model(content: bytes, kind: str = "file", path: str = "weights.onnx") -> dict:
    return {"sha256": hashlib.sha256(content).hexdigest(), "format": kind, "entrypoint": path}


def zipped(files: dict) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for path, data in files.items():
            archive.writestr(path, data)
    return output.getvalue()


def test_model_zip_keeps_weights_and_supporting_files() -> None:
    content = zipped({"weights/model.pt": b"weights", "config.json": b"{}"})
    output = io.BytesIO()
    artifacts.make_tar(
        io.BytesIO(content), output, model(content, "zip", "weights/model.pt"), 10000
    )
    with tarfile.open(fileobj=output) as archive:
        assert archive.extractfile("weights/model.pt").read() == b"weights"
        assert archive.extractfile("config.json").read() == b"{}"
        assert all(member.isfile() and member.mode == 0o444 for member in archive)


@pytest.mark.parametrize("path", ["../escape", "/root", "foo\\bar", "a//b", "a/./b", "C:drive"])
def test_zip_rejects_unsafe_paths(path: str) -> None:
    content = zipped({path: b"weights", "valid": b"ok"})
    with pytest.raises(ValueError, match="safe"):
        artifacts.make_tar(io.BytesIO(content), io.BytesIO(), model(content, "zip", "valid"), 10000)


def test_zip_rejects_symlinks_missing_entry_and_expansion() -> None:
    source = io.BytesIO()
    with zipfile.ZipFile(source, "w") as archive:
        member = zipfile.ZipInfo("link")
        member.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(member, "../outside")
    source.seek(0)
    with pytest.raises(ValueError, match="regular files"):
        artifacts.make_tar(source, io.BytesIO(), model(b"", "zip", "link"), 10000)
    content = zipped({"weights": b"weights"})
    with pytest.raises(ValueError, match="missing"):
        artifacts.make_tar(
            io.BytesIO(content), io.BytesIO(), model(content, "zip", "absent"), 10000
        )
    with pytest.raises(ValueError, match="size limit"):
        artifacts.make_tar(io.BytesIO(content), io.BytesIO(), model(content, "zip", "weights"), 3)


def test_download_pins_size_checksum_and_rejects_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    content = b"onnx-weights"
    assigned = model(content)
    grant = {
        "url": "https://storage.example/file?signature=secret",
        "sha256": assigned["sha256"],
        "size_bytes": len(content),
    }
    client = httpx.Client
    response = {"status": 200, "content": content}
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda **kw: client(
            **kw,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(response["status"], content=response["content"])
            ),
        ),
    )
    output = io.BytesIO()
    artifacts.download(grant, assigned, output, 1000)
    assert output.read() == content
    response["content"] = b"bad"
    with pytest.raises(ValueError, match="checksum"):
        artifacts.download(grant, assigned, io.BytesIO(), 1000)
    response["status"] = 302
    with pytest.raises(RuntimeError, match="download failed"):
        artifacts.download(grant, assigned, io.BytesIO(), 1000)
    with pytest.raises(ValueError, match="HTTPS"):
        artifacts.download({**grant, "url": "http://host/file"}, assigned, io.BytesIO(), 1000)


@pytest.mark.parametrize("mode", ["none", "bundled", "external"])
def test_depot_models_do_not_bypass_software_interface_validation(mode: str) -> None:
    from src.sink.publish.applications import contract

    software = {"version": "1", "image": "registry/app@sha256:" + "a" * 64, "model_mode": mode}
    if mode == "external":
        software["model_contract"] = "wrong-interface"
    spec = {
        "schema": contract.SCHEMA,
        "name": "test",
        "version": "1",
        "kind": "software_model",
        "application": "app",
        "runtime": "docker-application-v1",
        "platforms": ["linux/amd64"],
        "software": software,
        "model": {
            **model(b"weights"),
            "version": "1",
            "uri": "fleet://models/" + "b" * 64,
            "contract": "required-interface",
        },
    }
    with pytest.raises(ValueError, match="interfaces must match"):
        contract.validate(spec)


def test_model_copy_timeout_is_a_reportable_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    from src.sink.publish.applications.runtime import DockerRuntime

    def timeout(*args: object, **kwargs: object) -> None:
        raise subprocess.TimeoutExpired(["docker", "cp"], 300)

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(RuntimeError, match="staging timed out"):
        DockerRuntime.copy_model(io.BytesIO(), "seed")


def test_corrupt_deflate_is_reported_without_crashing_worker() -> None:
    import struct

    source = io.BytesIO()
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("weights.pt", b"weights" * 100)
    content = bytearray(source.getvalue())
    # Keep the valid ZIP directory/header, but replace its compressed payload.
    name_length, extra_length = struct.unpack_from("<HH", content, 26)
    data_offset = 30 + name_length + extra_length
    content[data_offset] = 0xFF  # Invalid DEFLATE block type.
    with pytest.raises(ValueError, match="corrupt"):
        artifacts.make_tar(
            io.BytesIO(content), io.BytesIO(), model(bytes(content), "zip", "weights.pt"), 10000
        )


def test_lzma_zip_is_rejected_before_decompression() -> None:
    source = io.BytesIO()
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_LZMA) as archive:
        archive.writestr("weights.pt", b"weights")
    source.seek(0)
    with pytest.raises(ValueError, match="DEFLATE"):
        artifacts.make_tar(
            source, io.BytesIO(), model(source.getvalue(), "zip", "weights.pt"), 10000
        )


def test_staging_recovery_removes_abandoned_volume_but_preserves_running_models(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test.sink.publish.applications.test_application_runtime import Engine, runtime

    app = runtime(tmp_path, Engine())
    commands = []

    def run(args: list[str], **kwargs: object) -> bytes:
        commands.append(args)
        return b"orphan\n" if args[:2] == ["volume", "ls"] else b""

    monkeypatch.setattr(app, "run", run)
    app.state["staged"] = {"volume": "orphan", "seed": "seed"}
    app.save()
    app.recover()
    assert ["volume", "rm", "orphan"] in commands
    assert "staged" not in app.state
    commands.clear()
    app.state.update(staged={"volume": "orphan", "seed": "seed"}, active={"volume": "orphan"})
    app.discard_staged()
    assert ["volume", "rm", "orphan"] not in commands
    assert app.state["active"]["volume"] == "orphan"
