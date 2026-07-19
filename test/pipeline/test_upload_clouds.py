"""Tests for the GCS and Azure upload tasks, using mocked SDK clients."""

import pathlib
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("google.cloud.storage")
pytest.importorskip("azure.storage.blob")

from azure.core.exceptions import ResourceNotFoundError

from bagel.pipeline.tasks.upload.azure import UploadFilesToAzure, md5_digest
from bagel.pipeline.tasks.upload.gcs import UploadFilesToGcs, md5_base64

# -- GCS -----------------------------------------------------------------------------


def _gcs_client(remote_md5: str | None = None) -> MagicMock:
    """A fake GCS client: blob missing by default, or present with an md5."""
    client = MagicMock()
    bucket = client.bucket.return_value
    if remote_md5 is None:
        bucket.get_blob.return_value = None
    else:
        bucket.get_blob.return_value = MagicMock(md5_hash=remote_md5)
    return client


def _gcs_uploaded_keys(client: MagicMock) -> list[str]:
    return [call.args[0] for call in client.bucket.return_value.blob.call_args_list]


def test_gcs_uploads_directory_under_prefix(tmp_path: pathlib.Path) -> None:
    (tmp_path / "a.csv").write_text("a")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.csv").write_text("b")

    client = _gcs_client()
    task = UploadFilesToGcs(bucket="bkt", source=str(tmp_path), prefix="fleet/run1")
    with patch("bagel.pipeline.tasks.upload.gcs.storage.Client", return_value=client):
        task.execute(asof_seconds=1e12, lookback=None)

    assert _gcs_uploaded_keys(client) == ["fleet/run1/a.csv", "fleet/run1/sub/b.csv"]


def test_gcs_skips_matching_md5(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "same.bin"
    source.write_bytes(b"identical")

    client = _gcs_client(remote_md5=md5_base64(source))
    task = UploadFilesToGcs(bucket="bkt", source=str(source))
    with patch("bagel.pipeline.tasks.upload.gcs.storage.Client", return_value=client):
        task.execute(asof_seconds=1e12, lookback=None)

    client.bucket.return_value.blob.assert_not_called()


def test_gcs_uploads_when_md5_differs(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "changed.bin"
    source.write_bytes(b"new content")

    client = _gcs_client(remote_md5="c29tZXRoaW5nIGVsc2U=")
    task = UploadFilesToGcs(bucket="bkt", source=str(source))
    with patch("bagel.pipeline.tasks.upload.gcs.storage.Client", return_value=client):
        task.execute(asof_seconds=1e12, lookback=None)

    assert _gcs_uploaded_keys(client) == ["changed.bin"]


def test_gcs_empty_bucket_rejected() -> None:
    with pytest.raises(ValueError, match="bucket"):
        UploadFilesToGcs(bucket="", source="x")


# -- Azure ---------------------------------------------------------------------------


def _azure_client(remote_md5: bytes | None = None) -> MagicMock:
    """A fake Azure container client: blob missing by default, or present with an md5."""
    client = MagicMock()
    blob = client.get_blob_client.return_value
    if remote_md5 is None:
        blob.get_blob_properties.side_effect = ResourceNotFoundError("missing")
    else:
        blob.get_blob_properties.return_value = MagicMock(
            content_settings=MagicMock(content_md5=bytearray(remote_md5))
        )
    return client


def test_azure_uploads_with_content_md5(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "reduced.mcap"
    source.write_bytes(b"payload")

    client = _azure_client()
    task = UploadFilesToAzure(container="ctr", source=str(source), connection_string="cs")
    with patch(
        "bagel.pipeline.tasks.upload.azure.ContainerClient.from_connection_string",
        return_value=client,
    ):
        task.execute(asof_seconds=1e12, lookback=None)

    blob = client.get_blob_client.return_value
    blob.upload_blob.assert_called_once()
    settings_arg = blob.upload_blob.call_args.kwargs["content_settings"]
    assert bytes(settings_arg.content_md5) == md5_digest(source)


def test_azure_skips_matching_md5(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "same.bin"
    source.write_bytes(b"identical")

    client = _azure_client(remote_md5=md5_digest(source))
    task = UploadFilesToAzure(container="ctr", source=str(source), connection_string="cs")
    with patch(
        "bagel.pipeline.tasks.upload.azure.ContainerClient.from_connection_string",
        return_value=client,
    ):
        task.execute(asof_seconds=1e12, lookback=None)

    client.get_blob_client.return_value.upload_blob.assert_not_called()


def test_azure_requires_connection_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(ValueError, match="connection string"):
        UploadFilesToAzure(container="ctr", source="x")


def test_registry_discovers_all_upload_tasks() -> None:
    from bagel.pipeline import capabilities

    modules = {
        entry["module"]: entry
        for entry in capabilities.list_capabilities()
        if entry["module"].startswith("bagel.pipeline.tasks.upload.")
        and entry["module"] != "bagel.pipeline.tasks.upload.base"
    }
    assert set(modules) == {
        "bagel.pipeline.tasks.upload.s3",
        "bagel.pipeline.tasks.upload.gcs",
        "bagel.pipeline.tasks.upload.azure",
    }
    assert all(entry["kind"] == "task" for entry in modules.values())
