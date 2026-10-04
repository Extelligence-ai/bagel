"""Artifact transfer, integrity and safe archive conversion without model execution."""

import hashlib
import io
import stat
import tarfile
import zipfile

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
