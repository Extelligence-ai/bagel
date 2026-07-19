"""Upload local files to an Azure Blob Storage container."""

import hashlib
import os
import pathlib

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient, ContentSettings

from bagel.di import module
from bagel.pipeline.tasks.upload import base


def md5_digest(local_file: pathlib.Path) -> bytes:
    """Return the raw MD5 digest of a file (Azure's Content-MD5 format)."""
    digest = hashlib.md5(usedforsecurity=False)
    with open(local_file, "rb") as stream:
        while chunk := stream.read(64 * 1024 * 1024):
            digest.update(chunk)
    return digest.digest()


class UploadFilesToAzure(base.UploadFilesTask):
    """Upload local files to an Azure Blob Storage container.

    See `UploadFilesTask` for the shared source/prefix/window/skip semantics; files
    are skipped when their MD5 matches the remote blob's Content-MD5 (which this task
    sets on upload). Authentication uses a connection string, passed explicitly or via
    the AZURE_STORAGE_CONNECTION_STRING environment variable.
    """

    def __init__(  # noqa: PLR0913
        self,
        container: str,
        source: str,
        prefix: str = "",
        connection_string: str | None = None,
        filter_modified_at: bool = False,
        skip_existing: bool = True,
    ) -> None:
        """Initialize the task.

        Args:
            container (str): The destination container name.
            source (str): A file path, directory (uploaded recursively), or glob pattern
                selecting the local files to upload.
            prefix (str, optional): Blob-name prefix under which objects are stored.
                Defaults to "" (container root).
            connection_string (str | None, optional): Storage-account connection string.
                If None, the AZURE_STORAGE_CONNECTION_STRING environment variable is
                used. Defaults to None.
            filter_modified_at (bool, optional): If True, only upload files whose
                modified time falls within `[asof_seconds - lookback, asof_seconds]`.
                Defaults to False.
            skip_existing (bool, optional): If True, skip files whose MD5 matches the
                existing remote blob's Content-MD5. Defaults to True.

        Raises:
            ValueError: If 'container' or 'source' is empty, or no connection string
                is available.

        """
        if not container:
            raise ValueError("'container' must not be empty.")
        super().__init__(
            source=source,
            prefix=prefix,
            filter_modified_at=filter_modified_at,
            skip_existing=skip_existing,
        )
        self._container = container
        self._connection_string = connection_string or os.environ.get(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        if not self._connection_string:
            raise ValueError(
                "No connection string: pass 'connection_string' or set "
                "AZURE_STORAGE_CONNECTION_STRING."
            )

    def _connect(self) -> object:
        return ContainerClient.from_connection_string(self._connection_string, self._container)

    def _destination(self) -> str:
        return f"azure://{self._container}/{self._prefix}"

    def _remote_matches(self, client: object, local_file: pathlib.Path, key: str) -> bool:
        try:
            properties = client.get_blob_client(key).get_blob_properties()
        except ResourceNotFoundError:
            return False
        remote_md5 = properties.content_settings.content_md5
        return remote_md5 is not None and bytes(remote_md5) == md5_digest(local_file)

    def _upload_file(self, client: object, local_file: pathlib.Path, key: str) -> None:
        with open(local_file, "rb") as stream:
            client.get_blob_client(key).upload_blob(
                stream,
                overwrite=True,
                content_settings=ContentSettings(content_md5=md5_digest(local_file)),
            )


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = UploadFilesToAzure
