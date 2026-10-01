"""Restricted Docker application installer with a durable recovery journal.

Operator configuration owns registry allowlists, readiness and resource limits.
A remote release cannot grant devices, host mounts, capabilities or a shell.
External models are OCI images containing exactly /model/model.bin; that file
is checksummed and mounted read-only into the application at /opt/fleet-model.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tarfile
import time
from collections.abc import Callable
from pathlib import Path
from threading import Timer

from src.sink.publish.applications import contract
from src.sink.publish.delivery.main import atomic

MIN_REGISTRY_PREFIX = 3
DOCKER = shutil.which("docker") or "/usr/local/bin/docker"

RUNTIME = "docker-application-v1"


class DockerRuntime:
    """Manage only application containers owned by this enrolled installation."""

    def __init__(
        self,
        root: Path | str,
        application: str,
        config: dict,
        binding: dict,
        run: Callable | None = None,
    ) -> None:
        """Bind local state to one installation and recover interrupted activation."""
        contract.identifier(application, "Application")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.application, self.config, self.binding = application, config, binding
        prefixes = config.get("registry_prefixes")
        if (
            not isinstance(prefixes, list)
            or not prefixes
            or any(
                not isinstance(p, str) or not p.endswith("/") or len(p) < MIN_REGISTRY_PREFIX
                for p in prefixes
            )
        ):
            raise ValueError("Configure explicit registry/repository prefixes ending in /.")
        if not isinstance(config.get("ready_file"), str):
            raise ValueError("A local readiness file is required.")
        self.path = self.root / "runtime.json"
        self.state = (
            json.loads(self.path.read_text()) if self.path.exists() else {"binding": binding}
        )
        if self.state["binding"] != binding:
            raise ValueError(
                "Installation identity changed; reconcile local applications before continuing."
            )
        self.run = run or self._run
        self.prefix = "bagel-app-" + contract.digest(binding)[:12] + "-" + application
        self.platform = (
            self.run(["info", "--format", "{{.OSType}}/{{.Architecture}}"])
            .decode()
            .strip()
            .replace("/aarch64", "/arm64")
            .replace("/x86_64", "/amd64")
        )
        self.recover()

    @staticmethod
    def _run(args: list[str], timeout: int = 300) -> bytes:
        try:
            result = subprocess.run(  # noqa: S603 -- fixed Docker CLI, validated argv, no shell
                [DOCKER, *args], capture_output=True, timeout=timeout, check=False
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("Docker operation timed out: " + args[0]) from exc
        if result.returncode:
            # Docker stderr can include registry URLs/credentials. Keep it local
            # and out of fleet reports; report only the operation that failed.
            raise RuntimeError("Docker operation failed: " + args[0])
        return result.stdout

    def save(self) -> None:
        """Flush the recovery journal to disk."""
        atomic(self.path, self.state)

    def inspect(self, name: str) -> dict | None:
        """Inspect a managed container without changing it."""
        try:
            return json.loads(self.run(["container", "inspect", name]))[0]
        except RuntimeError:
            return None

    def ready(self) -> bool:
        """Read the operator-owned activation readiness signal."""
        path = Path(self.config["ready_file"])
        return path.is_file() and path.read_text().strip() == "ready"

    def current(self) -> tuple[dict | None, bool]:
        """Report the inspected running pair and container health."""
        active = self.state.get("active")
        if not active:
            return None, False
        inspected = self.inspect(active["container"])
        if (
            not inspected
            or inspected["Config"]["Labels"].get("bagel.release") != contract.digest(active["pair"])
            or inspected["Config"]["Image"] != active["pair"]["software"]["image"]
        ):
            return None, False
        healthy = (
            inspected["State"]["Running"]
            and inspected["State"].get("Health", {}).get("Status") == "healthy"
        )
        return active["pair"], bool(healthy)

    def inventory(self) -> dict:
        """Describe installer capability and current application state."""
        pair, healthy = self.current()
        return {
            "runtime": RUNTIME,
            "platform": self.platform,
            "ready": self.ready(),
            "healthy": healthy,
            "current": pair,
        }

    def allowed(self, image: str) -> None:
        """Enforce immutable image references and local registry allowlists."""
        if (
            not isinstance(image, str)
            or not contract.IMAGE.fullmatch(image)
            or not any(image.startswith(p) for p in self.config["registry_prefixes"])
        ):
            raise ValueError(
                "Artifact must have an immutable digest in a locally approved registry/repository."
            )

    def pull(self, image: str) -> dict:
        """Download the platform image and verify its registry digest."""
        self.allowed(image)
        self.run(["pull", "--platform", self.platform, image], timeout=900)
        inspected = json.loads(self.run(["image", "inspect", image]))[0]
        sha = image.split("@")[1]
        if not any(ref.endswith("@" + sha) for ref in inspected.get("RepoDigests", [])):
            raise ValueError("Downloaded image digest could not be verified.")
        return inspected

    def prepare(self, pair: dict, target_id: str) -> dict:
        """Stage verified artifacts without interrupting the current application."""
        contract.validate_state(pair)
        inspected = self.pull(pair["software"]["image"])
        check = inspected.get("Config", {}).get("Healthcheck", {}).get("Test", [])
        if not check or check[0] == "NONE":
            raise ValueError("Application image requires a HEALTHCHECK before fleet deployment.")
        model = pair["model"]
        volume = None
        if model:
            if not model["uri"].startswith("oci://"):
                raise ValueError(
                    "This installer accepts models as OCI images containing /model/model.bin."
                )
            image = model["uri"][6:]
            self.pull(image)
            volume = self.prefix + "-model-" + contract.digest(model)[:20]
            seed = volume + "-seed"
            if not self.inspect(seed):
                self.run(
                    [
                        "create",
                        "--name",
                        seed,
                        "--network",
                        "none",
                        "--mount",
                        f"type=volume,src={volume},dst=/model",
                        image,
                        "/never-executed",
                    ]
                )
            # Docker cp streams a tar archive. Read the member without extracting
            # paths or symlinks to the host. Never execute the model image.
            if self.model_digest(seed) != model["sha256"]:
                raise ValueError("Model artifact checksum mismatch.")
        name = self.prefix + "-" + contract.digest(target_id)[:16]
        return {"container": name, "pair": pair, "volume": volume}

    def model_digest(self, seed: str) -> str:
        """Hash one bounded model file without extracting archive paths."""
        process = subprocess.Popen(  # noqa: S603 -- fixed CLI and locally generated container name
            [DOCKER, "cp", seed + ":/model/model.bin", "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        timer = Timer(self.config.get("model_read_timeout_seconds", 120), process.kill)
        timer.start()
        try:
            with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
                member = archive.next()
                if (
                    member is None
                    or not member.isfile()
                    or member.size > self.config.get("model_max_bytes", 536870912)
                ):
                    raise ValueError(
                        "Model must be a regular file within the configured size limit."
                    )
                sha = hashlib.sha256()
                reader = archive.extractfile(member)
                while chunk := reader.read(1024 * 1024):
                    sha.update(chunk)
                if archive.next() is not None:
                    raise ValueError("Expected exactly one model file.")
            if process.wait(timeout=30):
                raise RuntimeError("Could not read the model artifact.")
            return sha.hexdigest()
        except (tarfile.TarError, OSError) as exc:
            raise RuntimeError("Could not read the model artifact.") from exc
        finally:
            timer.cancel()
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()

    def wait_healthy(self, name: str) -> None:
        """Wait for an actual Docker health check or raise on failure."""
        deadline = time.monotonic() + self.config.get("health_timeout_seconds", 120)
        while time.monotonic() < deadline:
            current = self.inspect(name)
            if not current or not current["State"]["Running"]:
                raise RuntimeError("Application exited during activation.")
            status = current["State"].get("Health", {}).get("Status")
            if status == "healthy":
                return
            if status == "unhealthy":
                raise RuntimeError("Application health check failed.")
            time.sleep(1)
        raise RuntimeError("Application health check timed out.")

    def activate(self, prepared: dict, *, recovery: bool = False) -> None:
        """Activate a staged pair and recover the prior container on failure."""
        if not self.ready():
            raise ValueError("Robot is no longer ready for activation.")
        displaced = self.state.get("active")
        previous = displaced
        if previous and not self.current()[1]:
            if not recovery:
                raise ValueError("Current application is unhealthy; reconcile it before an update.")
            previous = None
        self.state["transition"] = {"previous": previous, "next": prepared}
        self.save()  # Durable intent precedes stopping or creating containers.
        try:
            if self.inspect(prepared["container"]):
                raise ValueError("Unexpected existing container for this attempt.")
            args = [
                "create",
                "--name",
                prepared["container"],
                "--label",
                "bagel.release=" + contract.digest(prepared["pair"]),
                "--label",
                "bagel.owner=" + contract.digest(self.binding),
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "256",
                "--memory",
                str(self.config.get("memory_bytes", 1073741824)),
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,size=67108864",  # noqa: S108 -- isolated container tmpfs
                "--restart",
                "unless-stopped",
            ]
            if prepared["volume"]:
                args += [
                    "--mount",
                    f"type=volume,src={prepared['volume']},dst=/opt/fleet-model,readonly",
                    "--env",
                    "FLEET_MODEL_PATH=/opt/fleet-model/model.bin",
                ]
            args += [prepared["pair"]["software"]["image"]]
            self.run(args)
            if displaced:
                self.run(["stop", "--time", "20", displaced["container"]])
            self.run(["start", prepared["container"]])
            self.wait_healthy(prepared["container"])
            committed = {**self.state, "active": prepared, "previous": previous}
            committed.pop("transition")
            atomic(self.path, committed)
            self.state = committed
        except Exception:
            self.recover()
            raise

    def recover(self) -> None:
        """Restore the prior container after interrupted activation without guessing success."""
        transition = self.state.get("transition")
        if not transition:
            return
        new = transition["next"]["container"]
        if self.inspect(new):
            self.run(["stop", "--time", "20", new])
            self.run(["rm", new])
        previous = transition["previous"]
        if previous:
            # Recovery can finish while offline, under the same local readiness
            # policy. Keep the journal if readiness is unavailable and retry.
            if not self.ready():
                raise ValueError("Recovery is waiting for local readiness.")
            self.run(["start", previous["container"]])
            self.wait_healthy(previous["container"])
        recovered = {**self.state, "active": previous, "recovered": True}
        recovered.pop("transition")
        atomic(self.path, recovered)
        self.state = recovered
