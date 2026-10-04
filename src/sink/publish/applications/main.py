"""Supervised application delivery worker sharing the enrolled fleet identity."""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import time
from collections.abc import Callable
from datetime import datetime
from http import HTTPStatus
from pathlib import Path

import httpx

from src.sink.publish.applications import contract
from src.sink.publish.applications.runtime import DockerRuntime
from src.sink.publish.delivery.main import Channel as CaptureChannel
from src.sink.publish.delivery.main import atomic

log = logging.getLogger("fleet-applications")


class Channel(CaptureChannel):
    """Reuse certificate loading/rotation, with a separate application API."""

    def model_download(self, target_id: str) -> dict:
        """Use the existing device certificate to fetch an assigned model only."""
        from src.sink.publish.uploads import UploadClient

        return UploadClient(self.identity.parent).request(
            "model-download", {"target_id": target_id}
        )

    def request(self, method: str, route: str, body: dict | None = None) -> dict:
        """Send an application request using the current enrolled certificate."""
        _, url, context = self.config()
        with httpx.Client(verify=context, trust_env=False, timeout=15) as client:
            response = client.request(method, url + "/v1/applications/" + route, json=body)
            response.raise_for_status()
            return response.json()


class Agent:
    """Journal attempts, reconcile running containers, and fence every activation."""

    def __init__(
        self,
        root: Path | str,
        channel: Channel,
        config: dict,
        runtime_factory: Callable = DockerRuntime,
    ) -> None:
        """Load durable attempts without starting an application."""
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "agent.json"
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {"attempts": {}}
        self.channel, self.config, self.runtime_factory = channel, config, runtime_factory
        self.runtimes = {}
        # A journaled activation must recover before requiring cloud access.
        if self.state.get("binding"):
            for app, settings in self.config["applications"].items():
                self.runtimes[app] = self.runtime_factory(
                    self.root / app, app, settings, self.state["binding"]
                )

    def save(self) -> None:
        """Persist attempt state before effects or reports."""
        atomic(self.path, self.state)

    def observe(
        self, job: dict, status: str, current: dict | None, detail: str = "", health: str = ""
    ) -> None:
        """Persist and submit a sequenced runtime observation."""
        if status == "rolled_back" and not health:
            health = "Previous container HEALTHCHECK passed."
        attempt = self.state["attempts"].setdefault(job["target_id"], {"sequence": job["sequence"]})
        attempt["sequence"] = max(attempt["sequence"], job["sequence"]) + 1
        attempt["pending"] = {
            "target_id": job["target_id"],
            "sequence": attempt["sequence"],
            "status": status,
            "observed_digest": contract.digest(current) if current else None,
            "detail": detail[:1000],
            "health_detail": health[:1000],
        }
        self.save()
        self.flush(attempt)

    def flush(self, attempt: dict) -> None:
        """Retry the exact pending observation without changing its sequence."""
        if not attempt.get("pending"):
            return
        try:
            self.channel.request("POST", "report", attempt["pending"])
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == HTTPStatus.CONFLICT:
                attempt.pop("pending", None)
                self.save()
            raise
        if attempt["pending"]["status"] == "applying":
            attempt["admitted"] = True
        attempt.pop("pending", None)
        self.save()

    def tick(self) -> None:  # noqa: C901, PLR0912, PLR0915 -- ordered journal/effect state machine
        """Reconcile current identity, local state and the latest authorized attempts."""
        for runtime in self.runtimes.values():
            runtime.recover()
        response = self.channel.request("GET", "next")
        ident = response["identity"]
        tenant, robot = self.channel.current_identity()
        if (ident["tenant_id"], ident["robot_id"]) != (tenant, robot):
            raise ValueError("Fleet service returned a different robot identity.")
        binding = {k: ident[k] for k in ("tenant_id", "installation_id", "credential_generation")}
        if self.state.get("binding") and self.state["binding"] != binding:
            raise ValueError(
                "Installation changed; reconcile local applications before enabling this worker."
            )
        self.state["binding"] = binding
        self.save()
        for app, config in self.config["applications"].items():
            if app not in self.runtimes:
                self.runtimes[app] = self.runtime_factory(self.root / app, app, config, binding)
            self.runtimes[app].model_download = getattr(self.channel, "model_download", None)
            self.runtimes[app].recover()
            self.channel.request(
                "POST",
                "inventory",
                {"application": app, "inventory": self.runtimes[app].inventory()},
            )
        for job in response["jobs"]:
            app = job["application"]
            runtime = self.runtimes.get(app)
            if runtime is None:
                self.observe(job, "rejected", None, "Application is not locally configured.")
                continue
            attempt = self.state["attempts"].setdefault(
                job["target_id"], {"sequence": job["sequence"]}
            )
            self.flush(attempt)
            current, healthy = runtime.current()
            if job.get("admitted_at") and not attempt.get("admitted"):
                # The server admitted this target but its local admission journal
                # is absent. Observe/recover it; never start a second activation.
                attempt.update(admitted=True, started=True)
                self.save()
            if job["status"] in ("failed", "rejected", "rolled_back"):
                self.observe(
                    job,
                    "failed"
                    if job["status"] == "rolled_back"
                    and (not healthy or current != job["previous"])
                    else job["status"],
                    current,
                    "Attempt finished unsuccessfully; start a new rollout to retry.",
                )
                continue
            if current == job["desired"] and (
                healthy or not job.get("rollback_of") or attempt.get("started")
            ):
                if not job.get("admitted_at") and not attempt.get("admitted"):
                    # An already-running identical pair is a no-op, but still
                    # needs current server admission before satisfying a pilot.
                    self.observe(job, "applying", current)
                self.observe(
                    job,
                    "healthy" if healthy else "active",
                    current,
                    health="Container HEALTHCHECK passed." if healthy else "",
                )
                continue
            if attempt.get("started"):
                # A previous tick may have died inside activation. Runtime recovery
                # ran before inventory; never blindly reapply an interrupted job.
                status = (
                    "rolled_back"
                    if healthy and current == job["previous"] and attempt.get("admitted")
                    else "failed"
                )
                self.observe(
                    job,
                    status,
                    current,
                    "Previous attempt failed or was interrupted; start a new rollout to retry.",
                )
                continue
            try:
                if datetime.fromisoformat(job["expires_at"]) <= datetime.fromisoformat(
                    response["server_time"]
                ):
                    raise ValueError("Activation admission expired.")
                if contract.digest(runtime.inventory()) != job["expected_inventory"]:
                    raise ValueError("Robot state changed since preview; create a new rollout.")
                if (
                    contract.digest(contract.validate_state(job["desired"]))
                    != job["desired_digest"]
                ):
                    raise ValueError("Desired software/model pair checksum is invalid.")
                if (
                    job["spec"]["runtime"] != runtime.inventory()["runtime"]
                    or runtime.platform not in job["spec"]["platforms"]
                ):
                    raise ValueError("Runtime or platform is incompatible.")
                if not attempt.get("admitted"):
                    self.observe(job, "verifying", current)
                prepared = runtime.prepare(job["desired"], job["target_id"])
                # Recheck current state and server authorization after downloads,
                # immediately before any application stop/start effects.
                if contract.digest(runtime.inventory()) != job["expected_inventory"]:
                    raise ValueError("Robot readiness or running state changed while staging.")
                self.channel.request(
                    "POST", "inventory", {"application": app, "inventory": runtime.inventory()}
                )
                self.observe(job, "applying", current)
                attempt["started"] = True
                self.save()
                runtime.activate(prepared, recovery=bool(job.get("rollback_of")))
            except (ValueError, RuntimeError) as exc:
                if hasattr(runtime, "discard_staged"):
                    runtime.discard_staged()
                attempt["started"] = True
                self.save()
                current, healthy = runtime.current()
                status = (
                    "rolled_back"
                    if healthy and current == job["previous"] and attempt.get("admitted")
                    else "failed"
                )
                self.channel.request(
                    "POST", "inventory", {"application": app, "inventory": runtime.inventory()}
                )
                self.observe(job, status, current, str(exc))
                continue
            current, healthy = runtime.current()
            self.channel.request(
                "POST", "inventory", {"application": app, "inventory": runtime.inventory()}
            )
            self.observe(
                job,
                "healthy" if healthy else "active",
                current,
                health="Container HEALTHCHECK passed." if healthy else "",
            )


def main() -> None:
    """Run the optional worker; the existing telemetry/capture process is independent."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--control-url")
    args = parser.parse_args()
    from settings import settings

    if not settings.FLEET_ENABLED:
        return
    config = json.loads(Path(args.config).read_text())
    if not isinstance(config.get("applications"), dict) or not config["applications"]:
        raise ValueError("Configure at least one local application.")
    root = Path(args.state)
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        agent = Agent(root, Channel(args.identity, args.control_url), config)
        while True:
            try:
                agent.tick()
            except (httpx.HTTPError, OSError, ValueError, RuntimeError):
                # Do not echo URLs, local secrets or server bodies. Current robot
                # applications keep running; failed authorization cannot admit work.
                log.warning(
                    "Application reconciliation unavailable; retaining current application state."
                )
            time.sleep(15)


if __name__ == "__main__":
    main()
