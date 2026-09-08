"""Snapshot live robot hardware into a WaffleForm via waffle-iron. EXPERIMENTAL BETA.

Runs the `waffle` CLI (https://github.com/arunvenkatadri/waffle-iron), which
auto-detects connected hardware, firmware, and software, and writes the state to
`robot.waffleform.yaml`. Each snapshot is copied into the artifact directory with
a timestamped name, so accumulated snaps make hardware state a queryable history.

Requires the waffle-iron binary on PATH: ``cargo install waffle-iron``.
"""

import logging
import pathlib
import shutil
import subprocess
from typing import Any

from settings import settings
from src.di import module
from src.pipeline import base

DEFAULT_TIMEOUT_SECONDS = 120


def run_waffle(
    workdir: str,
    binary: str = "waffle",
    init_if_missing: bool = True,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> pathlib.Path:
    """Run `waffle snap` (or `waffle init` on first contact) and return the form path.

    Raises:
        RuntimeError: If the waffle binary is not installed.
        subprocess.CalledProcessError: If the CLI exits non-zero.

    """
    executable = shutil.which(binary)
    if executable is None:
        raise RuntimeError(
            f"'{binary}' not found on PATH. Hardware snapshots need waffle-iron: "
            "cargo install waffle-iron (https://github.com/arunvenkatadri/waffle-iron)"
        )
    form = pathlib.Path(workdir) / "robot.waffleform.yaml"
    command = "init" if init_if_missing and not form.exists() else "snap"
    result = subprocess.run(  # noqa: S603 -- fixed argv, no shell; binary resolved via which
        [executable, command],
        cwd=workdir,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=True,
    )
    logging.info("waffle %s: %s", command, result.stdout.strip()[:200])
    if not form.exists():
        raise RuntimeError(f"waffle {command} completed but {form} was not written.")
    return form


class WaffleSnap(base.Task):
    """Snapshot live hardware state into a timestamped WaffleForm artifact.

    On each execution, runs `waffle snap` (auto-detecting hardware, firmware, and
    software versions) and copies the resulting WaffleForm into the artifact
    directory as `<asof>.waffleform.yaml` -- immediately queryable as a Bagel data
    source, and a growing history of the robot's hardware state.
    """

    def __init__(
        self,
        workdir: str = ".",
        binary: str = "waffle",
        init_if_missing: bool = True,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the task.

        Args:
            workdir (str, optional): Directory holding (or receiving) the robot's
                `robot.waffleform.yaml`. Defaults to the current directory.
            binary (str, optional): The waffle-iron executable name or path.
            init_if_missing (bool, optional): Run `waffle init` (scan and scaffold)
                when no WaffleForm exists yet; otherwise `waffle snap`.
            timeout_seconds (int, optional): CLI timeout.

        """
        self._workdir = workdir
        self._binary = binary
        self._init_if_missing = init_if_missing
        self._timeout_seconds = timeout_seconds

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Nothing to set up in this task."""

    def execute(self, asof_seconds: float, lookback: Any = None) -> list[pathlib.Path]:  # noqa: ANN401
        """Snapshot the hardware and archive the WaffleForm."""
        form = run_waffle(
            self._workdir,
            binary=self._binary,
            init_if_missing=self._init_if_missing,
            timeout_seconds=self._timeout_seconds,
        )
        directory = pathlib.Path(settings.ARTIFACT_DIRECTORY) / "waffle" / self.pipeline
        directory.mkdir(parents=True, exist_ok=True)
        # Keep the .waffleform.yaml suffix so every snapshot resolves as a data source.
        destination = directory / f"{asof_seconds:.0f}.waffleform.yaml"
        shutil.copyfile(form, destination)
        return [destination]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = WaffleSnap
