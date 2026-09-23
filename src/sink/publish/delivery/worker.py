"""One isolated Bagel runtime. Prepare first; subscribe only after activation.

stdout is a structured supervisor channel. Source credentials are read locally
and never emitted. Native diagnostics go to stderr in this first integration.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import sys
import threading
import uuid
from pathlib import Path

from src.sink.publish.delivery import contract


def emit(value: object) -> None:
    """Write one JSON supervisor event to stdout."""
    print(json.dumps(value), flush=True)  # noqa: T201 -- structured supervisor protocol


def main() -> None:  # noqa: C901, PLR0915 -- one worker preparation/activation lifecycle
    """Run the opt-in fleet capture process."""
    parent = os.getppid()
    job = json.loads(Path(sys.argv[1]).read_text())
    config = json.loads(Path(sys.argv[2]).read_text())
    directory = Path(sys.argv[3]).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    spec = contract.validate(job["spec"])
    source = config["sources"].get(spec["source"])
    if not source or source.get("sink") not in ("mqtt", "ros2.bridge"):
        raise ValueError("Source alias is not configured for this runtime.")
    if not set(spec["topics"]).issubset(set(source.get("topics", []))):
        raise ValueError("Requested topics are outside the locally approved source.")
    if contract.digest(spec) != job["digest"] or job["runtime"] != contract.RUNTIME:
        raise ValueError("Artifact digest or runtime contract mismatch.")
    # Pin the runtime contract at installation time. An older Bagel image must
    # fail preparation rather than silently treat an unsupported task as active.
    from settings import settings
    from src.di import module
    from src.pipeline import capabilities
    from src.pipeline.base import Pipeline

    allowed = "src.pipeline.tasks.write_topics_to_file"
    info = next((c for c in capabilities.list_capabilities(False) if c["module"] == allowed), None)
    if not info or not {"topics", "output_format", "flatten"}.issubset(
        {p["name"] for p in info["parameters"]}
    ):
        raise ValueError("Installed Bagel runtime lacks the required capture task.")
    # Per-process settings prevent different revisions sharing source buffers.
    settings.CACHE_DIRECTORY = str(directory / "cache")
    settings.ARTIFACT_DIRECTORY = str(directory / "artifacts")
    settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES = int(
        config.get("buffer_bytes_per_topic", 16 * 1024 * 1024)
    )
    settings.SINK_TOTAL_BUFFER_BYTES = settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES * 8
    quota = int(config.get("artifact_quota_bytes", 512 * 1024 * 1024))
    if quota < 1 or not 1 <= settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES <= 128 * 1024 * 1024:
        raise ValueError("Invalid local storage limits.")
    sink = module.provide(
        "src.sink." + source["sink"],
        {"host": source["host"], "port": source["port"], **source.get("args", {})},
    )
    try:
        missing = set(spec["topics"]) - set(sink.available_topics)
        if missing:
            raise ValueError("Required topics are unavailable.")
        pipeline = Pipeline.build(contract.native(spec, str(sink.directory), job["asset_id"]))
        original = pipeline.run_at
        run_lock = threading.Lock()

        def capture(asof: float) -> None:
            """Run a capture and report bounded local artifact evidence."""
            with run_lock:
                before = len(pipeline._produced)
                try:
                    used = sum(p.stat().st_size for p in directory.parent.rglob("*") if p.is_file())
                    if used >= quota:
                        raise RuntimeError("Local capture storage quota reached.")
                    original(asof)
                    artifacts = []
                    for produced in pipeline._produced[before:]:
                        path = Path(produced).resolve()
                        if not path.is_relative_to(directory):
                            raise RuntimeError("Runtime output escaped its managed directory.")
                        sha = hashlib.sha256()
                        with path.open("rb") as f:
                            for block in iter(lambda: f.read(1024 * 1024), b""):
                                sha.update(block)
                        sha = sha.hexdigest()
                        artifacts.append(
                            {
                                "path": str(path.relative_to(directory)),
                                "sha256": sha,
                                "bytes": path.stat().st_size,
                            }
                        )
                    emit(
                        {
                            "type": "execution",
                            "event_id": uuid.uuid4().hex,
                            "digest": job["digest"],
                            "asof": asof,
                            "status": "completed",
                            "artifacts": artifacts[:8],
                        }
                    )
                except Exception as exc:
                    emit(
                        {
                            "type": "execution",
                            "event_id": uuid.uuid4().hex,
                            "digest": job["digest"],
                            "asof": asof,
                            "status": "failed",
                            "error": type(exc).__name__,
                        }
                    )
                finally:
                    pipeline._produced.clear()

        pipeline.run_at = capture
        emit({"type": "ready", "digest": job["digest"]})
        if sys.stdin.readline().strip() != "activate":
            return
        # Subscribe the cadence topic last so all capture buffers exist first.
        sink.ensure_capacity(
            spec["topics"], buffer_size_bytes=settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES
        )
        order = [t for t in spec["topics"] if t != spec["trigger"]["topic"]] + [
            spec["trigger"]["topic"]
        ]
        for topic in order:
            sink.subscribe(
                topic,
                pipeline=pipeline if topic == spec["trigger"]["topic"] else None,
                buffer_size_bytes=settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES,
            )
        if set(sink.subscribed_topics) != set(spec["topics"]):
            raise RuntimeError("Subscription activation was incomplete.")
        emit({"type": "active", "digest": job["digest"]})
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        while not stop.wait(1):
            if sys.stdin.closed or os.getppid() != parent:
                break
    finally:
        sink.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Details may contain source credentials. Only a stable class crosses
        # the supervisor/cloud boundary; local operator config explains sources.
        emit({"type": "failed", "error": type(exc).__name__})
        sys.exit(1)
