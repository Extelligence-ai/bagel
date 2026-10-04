"""Real engine acceptance test; opt in with BAGEL_TEST_REGISTRY=localhost:5017.

The registry must be a disposable local registry. Only uniquely named test
containers/volumes/images are removed; no existing applications are touched.
"""

import hashlib
import json
import os
import uuid
from pathlib import Path

import pytest

from src.sink.publish.applications.runtime import DockerRuntime

REGISTRY = os.environ.get("BAGEL_TEST_REGISTRY")
pytestmark = pytest.mark.skipif(not REGISTRY, reason="disposable Docker registry not supplied")


def build(root: Path, tag: str, dockerfile: str, model: bytes) -> str:
    """Build and push a fixture, returning its immutable registry reference."""
    root.mkdir()
    (root / "Dockerfile").write_text(dockerfile)
    (root / "model.bin").write_bytes(model)
    DockerRuntime._run(["build", "-t", tag, str(root)])
    DockerRuntime._run(["push", tag])
    refs = json.loads(DockerRuntime._run(["image", "inspect", tag]))[0]["RepoDigests"]
    return next(ref for ref in refs if ref.startswith(tag.rsplit(":", 1)[0] + "@"))


def test_real_images_model_mount_health_rollback_and_offline_recovery(tmp_path: Path) -> None:  # noqa: PLR0915 -- one ordered acceptance scenario
    """Exercise actual OCI verification, activation and crash recovery."""
    registry = str(REGISTRY)
    prefix = registry + "/bagel-acceptance-" + uuid.uuid4().hex[:10]
    ready = tmp_path / "ready"
    ready.write_text("ready")
    config = {
        "registry_prefixes": [prefix + "/"],
        "ready_file": str(ready),
        "health_timeout_seconds": 15,
    }
    app = DockerRuntime(tmp_path / "state", "probe", config, {"test": prefix})
    image_tags = []
    try:
        # The probe loads its model at process start, then health verifies that
        # loaded bytes remain the expected version (not merely a running PID).
        dockerfile = """FROM alpine:latest
COPY model.bin /bundled-model
HEALTHCHECK --interval=1s --timeout=1s --retries=2 CMD \\
    cmp /tmp/loaded ${FLEET_MODEL_PATH:-/bundled-model} && \\
    test "$(cat /tmp/loaded)" != bad
CMD ["sh", "-c", "cat ${FLEET_MODEL_PATH:-/bundled-model} > /tmp/loaded; exec sleep 3600"]
"""
        image_tags.append(prefix + "/app:one")
        software = build(tmp_path / "app", image_tags[-1], dockerfile, b"bundled-v1")
        models = []
        for n, content in enumerate([b"external-v1", b"external-v2", b"bad"]):
            image_tags.append(prefix + f"/model:{n}")
            uri = build(
                tmp_path / f"model{n}",
                image_tags[-1],
                "FROM scratch\nCOPY model.bin /model/model.bin\n",
                content,
            )
            models.append(
                {
                    "version": str(n + 1),
                    "uri": "oci://" + uri,
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "contract": "probe-v1",
                }
            )
        bundled = {
            "software": {"version": "1", "image": software, "model_mode": "bundled"},
            "model": None,
        }
        app.activate(app.prepare(bundled, "bundled"))
        assert app.current() == (bundled, True)
        external = {
            "software": {
                "version": "2",
                "image": software,
                "model_mode": "external",
                "model_contract": "probe-v1",
            },
            "model": models[0],
        }
        app.activate(app.prepare(external, "coordinated"))
        assert app.current() == (external, True)
        updated = {**external, "model": models[1]}
        app.activate(app.prepare(updated, "model-only"))
        assert app.current() == (updated, True)
        inspected = app.inspect(app.state["active"]["container"])
        mount = next(m for m in inspected["Mounts"] if m["Destination"] == "/opt/fleet-model")
        assert mount["RW"] is False
        assert (
            app.run(["exec", app.state["active"]["container"], "cat", "/tmp/loaded"])  # noqa: S108 -- container tmpfs
            == b"external-v2"
        )
        with pytest.raises(RuntimeError, match="health check"):
            app.activate(app.prepare({**external, "model": models[2]}, "bad-model"))
        assert app.current() == (updated, True)
        with pytest.raises(ValueError, match="checksum"):
            app.prepare({**external, "model": {**models[0], "sha256": "0" * 64}}, "bad-checksum")
        assert app.current() == (updated, True)
        original_run = app.run
        candidate = app.prepare(bundled, "interrupted")

        def interrupted(args: list[str], timeout: int = 300) -> bytes:
            result = original_run(args, timeout=timeout)
            if args == ["start", candidate["container"]]:
                raise KeyboardInterrupt("simulated worker termination")
            return result

        app.run = interrupted
        with pytest.raises(KeyboardInterrupt):
            app.activate(candidate)
        # New process, same disk, no fleet service involved in recovery.
        app = DockerRuntime(app.root, "probe", config, {"test": prefix})
        assert app.current() == (updated, True)
        assert app.inspect(candidate["container"]) is None
        app.activate(app.prepare(bundled, "explicit-rollback"), recovery=True)
        assert app.current() == (bundled, True)
    finally:
        containers = (
            DockerRuntime._run(["ps", "-aq", "--filter", "name=" + app.prefix]).decode().split()
        )
        if containers:
            DockerRuntime._run(["rm", "-f", *containers])
        volumes = (
            DockerRuntime._run(["volume", "ls", "-q", "--filter", "name=" + app.prefix])
            .decode()
            .split()
        )
        if volumes:
            DockerRuntime._run(["volume", "rm", *volumes])
        for tag in image_tags:
            DockerRuntime._run(["image", "rm", tag])


def test_fleet_models_install_files_and_supporting_data_and_roll_back(  # noqa: PLR0915 -- real lifecycle
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both imported bytes and platform ZIPs actually load in the application."""
    import io
    import zipfile

    import httpx

    prefix = str(REGISTRY) + "/bagel-depot-" + uuid.uuid4().hex[:10]
    tag = prefix + "/app:one"
    ready = tmp_path / "ready"
    ready.write_text("ready")
    app = DockerRuntime(
        tmp_path / "state",
        "probe",
        {
            "registry_prefixes": [prefix + "/"],
            "ready_file": str(ready),
            "health_timeout_seconds": 20,
        },
        {"test": prefix},
    )
    payload = {"bytes": b"external-v1"}
    original_client = httpx.Client
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda **kw: original_client(
            **kw,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=payload["bytes"])
            ),
        ),
    )
    try:
        software = build(
            tmp_path / "app",
            tag,
            """FROM alpine:latest
HEALTHCHECK --interval=1s --timeout=1s --retries=2 CMD \\
    cmp /tmp/loaded "$FLEET_MODEL_PATH" && test "$(cat /tmp/loaded)" != bad
CMD ["sh", "-c", "cat $FLEET_MODEL_PATH > /tmp/loaded; exec sleep 3600"]
""",
            b"",
        )

        def pair(kind: str, entry: str, version: str) -> dict:
            sha = hashlib.sha256(payload["bytes"]).hexdigest()
            app.model_download = lambda target: {
                "url": "https://storage.example/model",
                "size_bytes": len(payload["bytes"]),
                "sha256": sha,
            }
            return {
                "software": {
                    "version": "1",
                    "image": software,
                    "model_mode": "external",
                    "model_contract": "test-v1",
                },
                "model": {
                    "version": version,
                    "uri": "fleet://models/" + sha,
                    "sha256": sha,
                    "contract": "test-v1",
                    "format": kind,
                    "entrypoint": entry,
                },
            }

        first = pair("file", "model.onnx", "1")
        app.activate(app.prepare(first, "external"))
        assert app.current() == (first, True)
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as zipped:
            zipped.writestr("weights/model.pt", b"trained-v2")
            zipped.writestr("config.json", b'{"classes":2}')
        payload["bytes"] = archive.getvalue()
        trained = pair("zip", "weights/model.pt", "2")
        app.activate(app.prepare(trained, "training"))
        container = app.state["active"]["container"]
        assert (
            app.run(["exec", container, "cat", "/opt/fleet-model/config.json"]) == b'{"classes":2}'
        )
        assert app.run(["exec", container, "cat", "/tmp/loaded"]) == b"trained-v2"  # noqa: S108 -- container tmpfs
        assert app.current() == (trained, True)
        payload["bytes"] = b"bad"
        broken = pair("file", "weights.onnx", "3")
        failed = app.prepare(broken, "unhealthy")
        with pytest.raises(RuntimeError, match="health check"):
            app.activate(failed)
        assert app.current() == (trained, True)
        volumes = app.run(["volume", "ls", "-q"]).decode().splitlines()
        assert failed["volume"] not in volumes
        assert app.state["active"]["volume"] in volumes
        payload["bytes"] = b"abandoned"
        abandoned = app.prepare(pair("file", "weights.onnx", "4"), "abandoned")
        app.recover()
        assert abandoned["volume"] not in app.run(["volume", "ls", "-q"]).decode().splitlines()
        assert app.current() == (trained, True)
        assert all(
            not member["RW"]
            for member in app.inspect(container)["Mounts"]
            if member["Destination"] == "/opt/fleet-model"
        )
    finally:
        containers = (
            DockerRuntime._run(["ps", "-aq", "--filter", "name=" + app.prefix]).decode().split()
        )
        if containers:
            DockerRuntime._run(["rm", "-f", *containers])
        volumes = (
            DockerRuntime._run(["volume", "ls", "-q", "--filter", "name=" + app.prefix])
            .decode()
            .split()
        )
        if volumes:
            DockerRuntime._run(["volume", "rm", *volumes])
        DockerRuntime._run(["image", "rm", tag])
