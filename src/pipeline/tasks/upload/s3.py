"""Upload local files to an S3 (or S3-compatible) bucket."""

import glob
import logging
import pathlib

import boto3
import botocore

from src import artifacts
from src.di import module
from src.pipeline import base

_GLOB_CHARS = "*?["


def _glob_root(pattern: str) -> pathlib.Path:
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


class UploadFilesToS3(base.Task):
    """Upload local files to an S3 (or S3-compatible) bucket.

    The `source` may be a single file, a directory (uploaded recursively), or a glob
    pattern. Object keys preserve the file structure relative to the source root, under
    an optional key `prefix`. Files whose SHA-256 checksum already matches the remote
    object are skipped, so re-running a pipeline does not re-upload unchanged artifacts.

    Works with any S3-compatible store (MinIO, Cloudflare R2, GCS interop, ...) via
    `endpoint_url`.
    """

    def __init__(  # noqa: PLR0913
        self,
        bucket: str,
        source: str,
        prefix: str = "",
        region: str | None = None,
        endpoint_url: str | None = None,
        filter_modified_at: bool = False,
        skip_existing: bool = True,
    ) -> None:
        """Initialize the task.

        Args:
            bucket (str): The destination bucket name.
            source (str): A file path, directory (uploaded recursively), or glob pattern
                selecting the local files to upload.
            prefix (str, optional): Key prefix under which objects are stored.
                Defaults to "" (bucket root).
            region (str | None, optional): The bucket region. If None, the default
                region resolution applies. Defaults to None.
            endpoint_url (str | None, optional): Endpoint for S3-compatible stores
                (e.g. MinIO). If None, AWS S3 is used. Defaults to None.
            filter_modified_at (bool, optional): If True, only upload files whose
                modified time falls within `[asof_seconds - lookback, asof_seconds]`
                when the task executes -- so a pipeline cadence uploads only new files.
                Defaults to False.
            skip_existing (bool, optional): If True, skip files whose SHA-256 checksum
                matches the existing remote object. Defaults to True.

        Raises:
            ValueError: If 'bucket' or 'source' is empty.

        """
        if not bucket:
            raise ValueError("'bucket' must not be empty.")
        if not source:
            raise ValueError("'source' must not be empty.")
        self._bucket = bucket
        self._source = source
        self._prefix = prefix.strip("/")
        self._region = region
        self._endpoint_url = endpoint_url
        self._filter_modified_at = filter_modified_at
        self._skip_existing = skip_existing

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Nothing to set up in this task."""

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
            root = _glob_root(self._source)
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

    def _remote_matches(self, s3_client: object, key: str, local_sha256: str) -> bool:
        """Return True if the remote object exists with the same SHA-256 checksum."""
        try:
            response = s3_client.get_object_attributes(
                Bucket=self._bucket, Key=key, ObjectAttributes=["Checksum"]
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return False
            raise
        return response.get("Checksum", {}).get("ChecksumSHA256") == local_sha256

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

        s3_client = boto3.client(
            "s3", region_name=self._region, endpoint_url=self._endpoint_url
        )

        uploaded, skipped = 0, 0
        for local_file, key in self._source_files():
            if self._filter_modified_at:
                modified_at = local_file.stat().st_mtime
                if start_seconds is not None and modified_at < start_seconds:
                    continue
                if modified_at > asof_seconds:
                    continue

            local_sha256 = artifacts.checksum_sha256(local_file)
            if self._skip_existing and self._remote_matches(s3_client, key, local_sha256):
                logging.info("'%s' already exists with matching SHA-256, skipping", key)
                skipped += 1
                continue

            s3_client.upload_file(
                Filename=str(local_file),
                Bucket=self._bucket,
                Key=key,
                ExtraArgs={"ChecksumAlgorithm": "SHA256"},
            )
            logging.info("Uploaded s3://%s/%s", self._bucket, key)
            uploaded += 1

        logging.info(
            "Upload complete: %d uploaded, %d skipped to s3://%s/%s",
            uploaded,
            skipped,
            self._bucket,
            self._prefix,
        )


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = UploadFilesToS3
