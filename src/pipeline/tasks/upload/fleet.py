"""Upload pipeline artifacts to the currently paired fleet workspace."""

import pathlib

from settings import settings
from src.di import module
from src.pipeline.tasks.upload import base
from src.sink.publish.uploads import UploadClient, manifest_for


class UploadFilesToFleet(base.UploadFilesTask):
    """Upload to the paired organization with run and clip provenance attached."""

    def __init__(
        self, source: str, run_id: str, origin: str, provenance: dict | None = None
    ) -> None:
        """Select files and their run; the device identity determines ownership.

        provenance accepts build_id, vcs_ref, capture times, trigger information,
        pipeline revision, and anomaly_labels (event records with time windows).
        Retries use the same content and manifest, including after a lost reply.
        The source files remain local on any failure so the pipeline can retry.
        """
        super().__init__(source=source, skip_existing=False)
        self._provenance = {**(provenance or {}), "run_id": run_id, "origin": origin}

    def _connect(self) -> UploadClient:
        return UploadClient(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

    def _destination(self) -> str:
        return "fleet://paired-workspace"

    def _remote_matches(self, client: object, local_file: pathlib.Path, key: str) -> bool:
        return False  # The authorization/confirmation exchange is idempotent.

    def _upload_file(self, client: UploadClient, local_file: pathlib.Path, key: str) -> None:
        client.upload(local_file, manifest_for(local_file, key, self._provenance))


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = UploadFilesToFleet
