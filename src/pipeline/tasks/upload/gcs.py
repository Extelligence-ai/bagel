"""Upload local files to a Google Cloud Storage bucket."""

import base64
import hashlib
import pathlib

from google.cloud import storage

from src.di import module
from src.pipeline.tasks.upload import base


def md5_base64(local_file: pathlib.Path) -> str:
    """Return the base64-encoded MD5 of a file (GCS's object checksum format)."""
    digest = hashlib.md5(usedforsecurity=False)
    with open(local_file, "rb") as stream:
        while chunk := stream.read(64 * 1024 * 1024):
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode("utf-8")


class UploadFilesToGcs(base.UploadFilesTask):
    """Upload local files to a Google Cloud Storage bucket.

    See `UploadFilesTask` for the shared source/prefix/window/skip semantics; files
    are skipped when their MD5 matches the remote object's. Credentials use the
    standard Google resolution chain (GOOGLE_APPLICATION_CREDENTIALS, gcloud ADC,
    workload identity).
    """

    def __init__(  # noqa: PLR0913
        self,
        bucket: str,
        source: str,
        prefix: str = "",
        project: str | None = None,
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
            project (str | None, optional): The GCP project. If None, the default
                project resolution applies. Defaults to None.
            filter_modified_at (bool, optional): If True, only upload files whose
                modified time falls within `[asof_seconds - lookback, asof_seconds]`.
                Defaults to False.
            skip_existing (bool, optional): If True, skip files whose MD5 matches the
                existing remote object. Defaults to True.

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
        self._project = project

    def _connect(self) -> object:
        return storage.Client(project=self._project)

    def _destination(self) -> str:
        return f"gs://{self._bucket}/{self._prefix}"

    def _remote_matches(self, client: object, local_file: pathlib.Path, key: str) -> bool:
        blob = client.bucket(self._bucket).get_blob(key)
        if blob is None:
            return False
        return blob.md5_hash == md5_base64(local_file)

    def _upload_file(self, client: object, local_file: pathlib.Path, key: str) -> None:
        client.bucket(self._bucket).blob(key).upload_from_filename(str(local_file))


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = UploadFilesToGcs
