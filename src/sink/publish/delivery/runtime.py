"""Bounded subprocess handoff. A prepared worker has not subscribed yet."""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path


class RuntimeFailureError(RuntimeError):
    """A native worker could not prepare or activate the requested capture."""

    pass


class Worker:
    """One isolated native capture runtime with an acknowledgment channel."""

    def __init__(self, job: dict, root: Path | str, sources: Path | str) -> None:
        """Initialize local state and the collaborators owned by this instance."""
        self.job = job
        self.root = Path(root) / job["digest"]
        self.root.mkdir(parents=True, exist_ok=True)
        manifest = self.root / "job.json"
        manifest.write_text(json.dumps(job))
        self.events = queue.Queue(maxsize=100)
        self.last_execution = {}
        # Share the supervisor session so forced server shutdown also kills workers.
        self.process = subprocess.Popen(  # noqa: S603 -- fixed worker module and local paths
            [
                sys.executable,
                "-m",
                "src.sink.publish.delivery.worker",
                str(manifest),
                str(sources),
                str(self.root),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        for line in self.process.stdout:
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    continue
                if event.get("type") == "execution":
                    self.last_execution = event
                else:
                    self.events.put_nowait(event)
            except (ValueError, queue.Full):
                continue

    def wait(self, kind: str, timeout: float = 30) -> None:
        """Wait for a matching worker acknowledgment within the deadline."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeFailureError("Bagel worker exited before activation.")
            try:
                event = self.events.get(timeout=0.1)
            except queue.Empty:
                continue
            if event.get("type") == "failed":
                raise RuntimeFailureError(
                    "Bagel preparation or activation failed: " + event.get("error", "unknown")
                )
            if event.get("type") == kind and event.get("digest") == self.job["digest"]:
                return
        raise RuntimeFailureError(
            "Bagel did not acknowledge " + kind + " within the activation deadline."
        )

    def activate(self) -> None:
        """Activate the prepared worker and preserve a recoverable previous release."""
        self.process.stdin.write("activate\n")
        self.process.stdin.flush()
        self.wait("active")

    def stop(self) -> None:
        """Stop the owned worker and wait for termination."""
        if self.process.poll() is None:
            try:
                self.process.terminate()
            except ProcessLookupError:
                return
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


class BagelRuntime:
    """Prepare and replace workers while preserving the prior release on failure."""

    def __init__(self, root: Path | str, sources: Path | str) -> None:
        """Initialize local state and the collaborators owned by this instance."""
        self.root, self.sources = root, sources
        self.worker = None

    @property
    def digest(self) -> str | None:
        """Return the active digest only while its worker is running."""
        return (
            self.worker.job["digest"]
            if self.worker and self.worker.process.poll() is None
            else None
        )

    @property
    def execution(self) -> dict:
        """Return the latest execution evidence from the current worker."""
        return self.worker.last_execution if self.worker else {}

    def inventory(self) -> dict:
        """Report local capture capabilities without connection details or credentials."""
        from src.sink.publish.delivery import contract

        config = json.loads(Path(self.sources).read_text())
        sources = {
            name: sorted(set(source.get("topics", [])))
            for name, source in config.get("sources", {}).items()
            if source.get("sink") in ("mqtt", "ros2.bridge")
        }
        return {
            "protocol": 2,
            "runtime": contract.RUNTIME,
            "sources": sources,
            "current_digest": self.digest,
        }

    def prepare(self, job: dict) -> Worker:
        """Build an isolated worker without activating subscriptions."""
        worker = Worker(job, self.root, self.sources)
        try:
            worker.wait("ready")
            return worker
        except Exception:
            worker.stop()
            raise

    def activate(self, prepared: Worker) -> None:
        """Activate the prepared worker and preserve a recoverable previous release."""
        old_job = self.worker.job if self.worker else None
        self.stop()
        try:
            prepared.activate()
            self.worker = prepared
        except Exception:
            prepared.stop()
            if old_job:
                previous = None
                try:
                    previous = self.prepare(old_job)
                    previous.activate()
                    self.worker = previous
                except Exception:
                    if previous:
                        previous.stop()
                    self.worker = None
            raise

    def restore(self, job: dict) -> None:
        """Restore only the last durable active release."""
        self.activate(self.prepare(job))

    def stop(self) -> None:
        """Stop the owned worker and wait for termination."""
        if self.worker:
            self.worker.stop()
            self.worker = None
