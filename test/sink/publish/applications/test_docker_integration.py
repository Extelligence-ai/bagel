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
    return json.loads(DockerRuntime._run(["image", "inspect", tag]))[0]["RepoDigests"][0]


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
