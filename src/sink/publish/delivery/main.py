"""mTLS polling, durable device journal and Bagel activation supervision."""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import signal
import ssl
import time
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urlparse

import httpx
import yaml

from src.sink.publish.delivery import contract
from src.sink.publish.delivery.runtime import BagelRuntime, RuntimeFailureError

MAX_POLL_SECONDS = 60

log = logging.getLogger("fleet-agent")


class SupersededError(Exception):
    """The server refused the report; reconciliation must precede any effect."""


def atomic(path: Path | str, value: object) -> None:
    """Persist a journal by atomic replacement, flushing file and directory."""
    path = Path(path)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as f:
        json.dump(value, f, sort_keys=True, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class Channel:
    """Authenticated control transport using the existing fleet identity."""

    def __init__(self, identity: Path | str, url: str | None = None) -> None:
        """Initialize local state and the collaborators owned by this instance."""
        self.identity = Path(identity)
        self.url = url

    def config(self) -> tuple[dict, str, ssl.SSLContext]:
        """Reload the enrolled certificate pointers and verified HTTPS context."""
        identity = yaml.safe_load(self.identity.read_text())
        url = (self.url or identity.get("renew_url") or "").rstrip("/")
        parsed = urlparse(url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Control requires an HTTPS base URL without embedded credentials.")
        root = self.identity.parent.resolve()
        paths = []
        for name, default in [
            ("ca_file", "ca.crt"),
            ("cert_file", "robot.crt"),
            ("key_file", "robot.key"),
        ]:
            path = (root / identity.get(name, default)).resolve()
            if not path.is_relative_to(root):
                raise ValueError("Identity files must remain inside the identity directory.")
            paths.append(str(path))
        context = ssl.create_default_context(cafile=paths[0])
        context.load_cert_chain(paths[1], paths[2])
        return identity, url, context

    def request(self, method: str, route: str, body: dict | None = None) -> dict:
        """Exchange one bounded control request using the current client certificate."""
        _, url, context = self.config()  # reload renewed certificates on each call
        with httpx.Client(verify=context, trust_env=False, timeout=10) as client:
            response = client.request(method, url + "/v1/control/" + route, json=body)
            response.raise_for_status()
            return response.json()

    def current_identity(self) -> tuple[str, str]:
        """Read the existing enrollment document through the fleet identity loader."""
        from src.sink.publish.identity import load_identity

        value = load_identity(self.identity.parent)
        return value.tenant, value.robot_id


class Agent:
    """Reconcile desired releases with durable local execution state."""

    def __init__(self, root: Path | str, channel: Channel, runtime: BagelRuntime) -> None:
        """Initialize local state and the collaborators owned by this instance."""
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "journal.json"
        self.channel, self.runtime = channel, runtime
        self.state = (
            json.loads(self.path.read_text())
            if self.path.exists()
            else {"sequence": 0, "seen_revision": 0}
        )

    def save(self) -> None:
        """Persist supervisor state before effects or acknowledgments."""
        atomic(self.path, self.state)

    def observe(self, job: dict, status: str, detail: str = "") -> None:
        """Journal and send a sequenced observation of the actual runtime."""
        self.state["sequence"] += 1
        self.state["pending_report"] = {
            "target_id": job["target_id"],
            "revision": job["revision"],
            "sequence": self.state["sequence"],
            "status": status,
            "observed_digest": self.runtime.digest,
            "detail": detail[:1000],
            "execution": self.runtime.execution,
        }
        self.save()
        self.flush()

    def flush(self) -> None:
        """Retry the pending report without changing its sequence or content."""
        body = self.state.get("pending_report")
        if not body:
            return
        try:
            self.channel.request("POST", "report", body)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != HTTPStatus.CONFLICT:
                raise
            # The authoritative head advanced; keep local execution history,
            # but never retry an acknowledgment against the wrong revision.
            self.state.pop("pending_report", None)
            self.save()
            raise SupersededError("Control revision changed; reconcile before activation.") from exc
        self.state.pop("pending_report", None)
        self.save()

    def _matches(self, job: dict) -> bool:
        tenant, robot = self.channel.current_identity()
        return job.get("tenant_id") == tenant and job.get("robot_id") == robot

    def restore(self) -> None:
        """Restore only the last durable active release."""
        job = self.state.get("active")
        if (
            job
            and not self.state.get("credential_denied")
            and self._matches(job)
            and contract.digest(contract.validate(job["spec"])) == job["digest"]
        ):
            self.runtime.restore(job)

    def deny(self) -> None:
        """Stop execution and retain credential denial across restarts."""
        self.runtime.stop()
        self.state["credential_denied"] = True
        self.save()

    def tick(self) -> None:  # noqa: C901, PLR0912, PLR0915 -- ordered journal/effect state machine
        """Reconcile one authoritative desired revision before activation."""
        response = self.channel.request("GET", "next")
        ident, job = response["identity"], response["job"]
        tenant, robot = self.channel.current_identity()
        if (ident["tenant_id"], ident["robot_id"]) != (tenant, robot):
            raise RuntimeFailureError("Control identity does not match the enrolled controller.")
        prior = self.state.get("installation_id")
        if prior and prior != ident["installation_id"]:
            self.runtime.stop()
            self.state = {"sequence": 0, "seen_revision": 0}
        self.state["installation_id"] = ident["installation_id"]
        self.state.pop("credential_denied", None)
        self.save()
        if not job:
            self.flush()
            return
        if (
            not self._matches(job)
            or job["installation_id"] != ident["installation_id"]
            or job["credential_generation"] != ident["credential_generation"]
        ):
            raise RuntimeFailureError("Job targets another installation or generation.")
        self.state["sequence"] = max(self.state["sequence"], job["last_sequence"])
        if job["revision"] < self.state["seen_revision"]:
            raise RuntimeFailureError("Refusing a stale desired revision.")
        self.flush()
        try:
            valid = (
                job["runtime"] == contract.RUNTIME
                and contract.digest(contract.validate(job["spec"])) == job["digest"]
            )
        except ValueError:
            valid = False
        if not valid:
            self.observe(job, "rejected", "Unsupported runtime or artifact digest mismatch.")
            return
        if self.runtime.digest == job["digest"]:
            self.state.update(active=job, desired=job, seen_revision=job["revision"])
            self.save()
            self.observe(job, "active")
            return
        if self.state.get("failed_target") == job["target_id"]:
            self.observe(
                job,
                "failed",
                "This attempt requires an explicit new deployment; "
                "previous pipeline may remain active.",
            )
            return
        self.state.update(desired=job, seen_revision=job["revision"])
        self.save()  # desired journal precedes every effect
        prepared = None
        try:
            now = datetime.fromisoformat(response["server_time"])
            expired = datetime.fromisoformat(job["expires_at"]) <= now
            known_active = self.state.get("active", {}).get("target_id") == job["target_id"]
            if expired and not known_active:
                self.observe(
                    job,
                    "rejected",
                    "Admission deadline expired; execution outcome is not inferred.",
                )
                self.state["failed_target"] = job["target_id"]
                self.save()
                return
            self.observe(job, "received")
            self.observe(job, "validating")
            prepared = self.runtime.prepare(job)
            # Reconcile after potentially slow topic discovery, before stopping
            # the working pipeline. Never activate a now-superseded target.
            latest = self.channel.request("GET", "next")
            if not latest["job"] or latest["job"]["target_id"] != job["target_id"]:
                prepared.stop()
                return
            if (
                datetime.fromisoformat(job["expires_at"])
                <= datetime.fromisoformat(latest["server_time"])
                and not known_active
            ):
                raise ValueError("Admission deadline expired during preparation.")
            self.observe(job, "applying")
            self.runtime.activate(prepared)
            prepared = None
            self.state["active"] = job
            self.state.pop("failed_target", None)
            self.save()  # durable active state precedes acknowledgment
            self.observe(job, "active")
        except (ValueError, RuntimeFailureError) as exc:
            if prepared:
                prepared.stop()
            self.state["failed_target"] = job["target_id"]
            self.save()
            self.observe(job, "failed", str(exc))
        except Exception:
            if prepared:
                prepared.stop()
            raise


def main() -> None:  # noqa: C901 -- bounded process lifecycle and recovery
    """Run the opt-in fleet capture process."""
    parser = argparse.ArgumentParser(
        description="Deliver approved fleet captures to a local Bagel runtime."
    )
    parser.add_argument("--identity", required=True)
    parser.add_argument("--sources", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--control-url")
    parser.add_argument("--poll-seconds", type=int, default=10)
    args = parser.parse_args()
    from src.sink.publish import require_fleet

    require_fleet()
    if not 1 <= args.poll_seconds <= MAX_POLL_SECONDS:
        parser.error("poll-seconds must be between 1 and 60")
    root = Path(args.state).resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / "agent.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    runtime = BagelRuntime(root / "releases", Path(args.sources).resolve())
    agent = Agent(root, Channel(args.identity, args.control_url), runtime)
    stopping = False

    def stop(*_: object) -> None:
        """Stop the owned worker and wait for termination."""
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        # Restore only the last committed active artifact; never an interrupted
        # pending activation. New desired state always requires an online pull.
        try:
            agent.restore()
        except Exception as exc:
            log.warning("Prior capture could not be restored: %s", type(exc).__name__)
        while not stopping:
            try:
                agent.tick()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == HTTPStatus.FORBIDDEN:
                    agent.deny()
                log.warning("Control request refused: HTTP %s", exc.response.status_code)
            except Exception as exc:
                from src.sink.publish import FleetNotEnrolledError

                if isinstance(exc, FleetNotEnrolledError | FileNotFoundError):
                    agent.deny()
                log.warning("Control reconciliation unavailable: %s", type(exc).__name__)
            for _ in range(args.poll_seconds * 5):
                if stopping:
                    break
                time.sleep(0.2)
    finally:
        runtime.stop()
        lock.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
