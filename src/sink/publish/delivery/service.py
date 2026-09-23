"""Manage the opt-in delivery supervisor alongside the existing fleet publisher."""

from __future__ import annotations

import atexit
import logging
import os
import pathlib
import signal
import subprocess
import sys
import threading

from settings import settings
from src.sink.publish import require_fleet

_process: subprocess.Popen | None = None
_lock = threading.RLock()


def start() -> bool:
    """Start once when configured; the supervisor shares the enrolled identity."""
    global _process  # noqa: PLW0603 -- process-wide server lifecycle handle
    if not settings.FLEET_DELIVERY_SOURCES or not settings.FLEET_ENABLED:
        return False
    require_fleet()
    with _lock:
        if _process is not None and _process.poll() is None:
            return True
        source = pathlib.Path(settings.FLEET_DELIVERY_SOURCES).resolve()
        if not source.is_file():
            raise ValueError("Fleet delivery source configuration is missing.")
        command = [
            sys.executable,
            "-m",
            "src.sink.publish.delivery.main",
            "--identity",
            str(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY) / "identity.yaml"),
            "--sources",
            str(source),
            "--state",
            settings.FLEET_DELIVERY_DIRECTORY,
        ]
        if settings.FLEET_CONTROL_URL:
            command += ["--control-url", settings.FLEET_CONTROL_URL]
        # Workers share this session. A forced stop kills the whole tree.
        _process = subprocess.Popen(command, start_new_session=True)  # noqa: S603 -- fixed module, local operator config
        return True


def stop() -> None:
    """Stop delivery before unenrollment or server exit, including its workers."""
    global _process
    with _lock:
        process, _process = _process, None
        if process is None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                logging.warning(
                    "Fleet delivery exceeded shutdown grace; stopping its process group"
                )
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        except ProcessLookupError:
            pass


atexit.register(stop)
