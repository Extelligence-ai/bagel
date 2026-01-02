"""Copy files either from a remote host or local path to a local directory using rsync."""

import json
import logging
import pathlib
import shutil
import subprocess
import tempfile
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from src.di import module
from src.pipeline import base

JSON_LINE_PREFIX = "__JSON_LINE: "
JSON_LINE_FORMAT = (
    "{"
    '"operation": "%o",'
    '"itemize": "%i",'
    '"file": "%n",'
    '"size_bytes": %l,'
    '"transferred_bytes": %b,'
    '"executed_time": "%t",'
    '"modified_time": "%M",'
    '"checksum": "%C"'
    "}"
)


class Operation(BaseModel):
    """Represent a single rsync operation."""

    op: str
    type: str
    source: str
    file: str
    destination: str
    size_bytes: int
    transferred_bytes: int
    executed_at: float  # in seconds
    modified_at: float  # in seconds
    checksum: str | None


class RsyncFilesToLocalDirectory(base.Task):
    """Copy files either from a remote host or local path to a local directory using rsync."""

    def __init__(
        self,
        source: str,
        directory: str,
        options: list[str] | None = None,
        filter_modified_at: bool = False,
    ) -> None:
        """Initialize the task.

        Args:
            source (str): The source path for rsync.
            directory (str): The local directory to sync files to.
            options (list[str] | None, optional): Additional rsync options. Defaults to None.
            filter_modified_at (bool, optional): Whether to filter files by modified time.
                If True, asof_seconds and lookback will be used to filter files based on their
                modified time when executing the task. Defaults to False.

        Raises:
            ValueError: If '--quiet' is provided in options.
            ValueError: If '--log-format' is provided in options.

        """
        self._source = source
        self._directory = directory

        options = options or []

        if any("--quiet" in option for option in options):
            raise ValueError("'--quiet' option is not allowed since output parsing is needed")

        if any("--log-format" in option for option in options):
            raise ValueError("'--log-format' is a reserved option that cannot be set")

        log_format = f"--log-format={JSON_LINE_PREFIX}{JSON_LINE_FORMAT}"
        self._options = [*options, log_format]

        self._filter_modified_at = filter_modified_at

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Nothing to setup in this task."""

    def _start_seconds(self, asof_seconds: float, lookback: base.Lookback | None) -> float | None:
        match lookback:
            case base.Lookback(unit=base.Unit.FRAME):
                raise ValueError(f"Does not support lookback with FRAME unit: {lookback}")
            case base.Lookback():
                return asof_seconds - lookback.to_seconds()
            case None:
                return None

    def _build_operation(self, data: dict[str, Any]) -> Operation:
        file = data["file"]
        dest_dir = pathlib.Path(self._directory).resolve()
        destination_path = (dest_dir / file).resolve()

        try:
            destination_path.relative_to(dest_dir)
        except ValueError as e:
            raise ValueError(f"'{file}' escapes destination directory '{dest_dir}'") from e

        return Operation(
            op=data["operation"],
            type=data["itemize"][1],
            source=self._source,
            file=file,
            destination=str(destination_path),
            size_bytes=data["size_bytes"],
            transferred_bytes=data["transferred_bytes"],
            executed_at=datetime.strptime(data["executed_time"], "%Y/%m/%d %H:%M:%S").timestamp(),
            modified_at=datetime.strptime(data["modified_time"], "%Y/%m/%d-%H:%M:%S").timestamp(),
            checksum=data["checksum"].strip() or None,
        )

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:  # noqa: C901
        """Execute the task at the given time."""
        if not self._filter_modified_at:
            logging.debug(
                "'asof_seconds' and 'lookback' are ignored since 'filter_modified_at' is False"
            )

        executed_operations = []
        start_seconds = self._start_seconds(asof_seconds, lookback)

        with tempfile.TemporaryDirectory() as temp_dir:
            command = ["rsync", *self._options, self._source, temp_dir]

            result = subprocess.run(  # noqa: S603
                command, capture_output=True, text=True, check=True
            )

            for line in result.stdout.splitlines():
                if not line.startswith(JSON_LINE_PREFIX):
                    continue

                try:
                    data = json.loads(line.removeprefix(JSON_LINE_PREFIX))
                    op = self._build_operation(data)

                    if op.type != "f":
                        continue

                    if self._filter_modified_at:
                        if start_seconds is not None and op.modified_at < start_seconds:
                            continue
                        if op.modified_at > asof_seconds:
                            continue

                    copied_file = pathlib.Path(temp_dir) / op.file
                    destination_path = pathlib.Path(op.destination)

                    if op.op in ("send", "recv"):
                        destination_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(copied_file, destination_path)
                    elif op.op == "del.":
                        destination_path.unlink(missing_ok=True)
                    else:
                        logging.warning("Unknown rsync operation: %s", op)
                        continue

                    logging.debug("rsync operation completed: %s", op)
                    executed_operations.append(op)

                except Exception as e:
                    logging.error("Failed to process operation from line: %s\nError: %s", line, e)
                    continue

        logging.info(f"{len(executed_operations)} rsync operations executed:")
        for op in executed_operations:
            logging.info(f"- {op.op} {op.destination} [{op.size_bytes} bytes]")


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = RsyncFilesToLocalDirectory
