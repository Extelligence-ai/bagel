"""Upload local files to an S3 (or S3-compatible) bucket."""

import pathlib

import boto3
import botocore

from src import artifacts
from src.di import module
from src.pipeline.tasks.upload import base


class UploadFilesToS3(base.UploadFilesTask):
    """Upload local files to an S3 (or S3-compatible) bucket.

    See `UploadFilesTask` for the shared source/prefix/window/skip semantics; files
    are skipped when their SHA-256 matches the remote object's checksum. Works with
    any S3-compatible store (MinIO, Cloudflare R2, GCS interop, ...) via `endpoint_url`.
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
                modified time falls within `[asof_seconds - lookback, asof_seconds]`.
                Defaults to False.
            skip_existing (bool, optional): If True, skip files whose SHA-256 checksum
                matches the existing remote object. Defaults to True.

        Raises:
            ValueError: If 'bucket' or 'source' is empty.

        """
        if not bucket:
            raise ValueError("'bucket' must not be empty.")
        super().__init__(
            source=source,
            prefix=prefix,
            filter_modified_at=filter_modified_at,
            skip_existing=skip_existing,
        )
        self._bucket = bucket
        self._region = region
        self._endpoint_url = endpoint_url

    def _connect(self) -> object:
        return boto3.client("s3", region_name=self._region, endpoint_url=self._endpoint_url)

    def _destination(self) -> str:
        return f"s3://{self._bucket}/{self._prefix}"

    def _remote_matches(self, client: object, local_file: pathlib.Path, key: str) -> bool:
        try:
            response = client.get_object_attributes(
                Bucket=self._bucket, Key=key, ObjectAttributes=["Checksum"]
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return False
            raise
        local_sha256 = artifacts.checksum_sha256(local_file)
        return response.get("Checksum", {}).get("ChecksumSHA256") == local_sha256

    def _upload_file(self, client: object, local_file: pathlib.Path, key: str) -> None:
        client.upload_file(
            Filename=str(local_file),
            Bucket=self._bucket,
            Key=key,
            ExtraArgs={"ChecksumAlgorithm": "SHA256"},
        )


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = UploadFilesToS3
