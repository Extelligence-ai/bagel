"""Shared skeleton for cloud upload tasks.

Providers (S3, GCS, Azure) share everything except the client, the checksum
comparison, and the transfer call: source resolution (file / directory / glob),
key layout under a prefix, the modified-time window, and skip-existing plumbing
live here.
"""

import abc
import glob
import logging
import pathlib

from src.pipeline import base

_GLOB_CHARS = "*?["


def glob_root(pattern: str) -> pathlib.Path:
    """Return the leading non-wildcard directory of a glob pattern.

    Used as the base against which matched files are relativized, so a pattern like
    ``logs/2026/*/imu.mcap`` produces keys relative to ``logs/2026``.
    """
    root_parts = []
    for part in pathlib.PurePath(pattern).parts:
        if any(char in part for char in _GLOB_CHARS):
            break
        root_parts.append(part)
    return pathlib.Path(*root_parts) if root_parts else pathlib.Path(".")


class UploadFilesTask(base.Task):
    """Base class for tasks that upload local files to a cloud object store.

    The `source` may be a single file, a directory (uploaded recursively), or a glob
    pattern. Object keys preserve the file structure relative to the source root, under
    an optional key `prefix`. Files whose checksum already matches the remote object
    are skipped, so re-running a pipeline does not re-upload unchanged artifacts.
    """

    def __init__(
        self,
        source: str,
        prefix: str = "",
        filter_modified_at: bool = False,
        skip_existing: bool = True,
    ) -> None:
        """Initialize the shared upload options.

        Args:
            source (str): A file path, directory (uploaded recursively), or glob pattern
                selecting the local files to upload.
            prefix (str, optional): Key prefix under which objects are stored.
                Defaults to "" (bucket/container root).
            filter_modified_at (bool, optional): If True, only upload files whose
                modified time falls within `[asof_seconds - lookback, asof_seconds]`
                when the task executes -- so a pipeline cadence uploads only new files.
                Defaults to False.
            skip_existing (bool, optional): If True, skip files whose checksum matches
                the existing remote object. Defaults to True.

        Raises:
            ValueError: If 'source' is empty.

        """
        if not source:
            raise ValueError("'source' must not be empty.")
        self._source = source
        self._prefix = prefix.strip("/")
        self._filter_modified_at = filter_modified_at
        self._skip_existing = skip_existing

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Nothing to set up in this task."""

    # -- provider hooks --------------------------------------------------------------

    @abc.abstractmethod
    def _connect(self) -> object:
        """Return the provider client."""

    @abc.abstractmethod
    def _destination(self) -> str:
        """Return the destination for logging, e.g. ``s3://bucket/prefix``."""

    @abc.abstractmethod
    def _remote_matches(self, client: object, local_file: pathlib.Path, key: str) -> bool:
        """Return True if the remote object exists with a matching checksum."""

    @abc.abstractmethod
    def _upload_file(self, client: object, local_file: pathlib.Path, key: str) -> None:
        """Transfer one file to the destination key."""

    # -- shared machinery ------------------------------------------------------------

    def _source_files(self) -> list[tuple[pathlib.Path, str]]:
        """Resolve the source into (local file, object key) pairs."""
        source = pathlib.Path(self._source)

        if source.is_file():
            pairs = [(source, source.name)]
        elif source.is_dir():
            pairs = [
                (file, file.relative_to(source).as_posix())
                for file in sorted(source.rglob("*"))
                if file.is_file()
            ]
        else:
            root = glob_root(self._source)
            pairs = [
                (match, match.relative_to(root).as_posix())
                for match in sorted(
                    pathlib.Path(p) for p in glob.glob(self._source, recursive=True)
                )
                if match.is_file()
            ]

        if self._prefix:
            pairs = [(file, f"{self._prefix}/{key}") for file, key in pairs]
        return pairs

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        """Execute the task at the given time."""
        start_seconds = None
        if self._filter_modified_at:
            match lookback:
                case base.Lookback(unit=base.Unit.FRAME):
                    raise ValueError(f"Does not support lookback with FRAME unit: {lookback}")
                case base.Lookback():
                    start_seconds = asof_seconds - lookback.to_seconds()
                case None:
                    start_seconds = None
        else:
            logging.debug(
                "'asof_seconds' and 'lookback' are ignored since 'filter_modified_at' is False"
            )

        client = self._connect()

        uploaded, skipped = 0, 0
        for local_file, key in self._source_files():
            if self._filter_modified_at:
                modified_at = local_file.stat().st_mtime
                if start_seconds is not None and modified_at < start_seconds:
                    continue
                if modified_at > asof_seconds:
                    continue

            if self._skip_existing and self._remote_matches(client, local_file, key):
                logging.info("'%s' already exists with matching checksum, skipping", key)
                skipped += 1
                continue

            self._upload_file(client, local_file, key)
            logging.info(
                "Uploaded %s/%s (%d bytes)",
                self._destination().rstrip("/"),
                key,
                local_file.stat().st_size,
            )
            uploaded += 1

        logging.info(
            "Upload complete: %d uploaded, %d skipped to %s",
            uploaded,
            skipped,
            self._destination(),
        )
