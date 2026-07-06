"""Tests for the S3 upload task, using a mocked boto3 client."""

import pathlib
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from src.pipeline import base
from src.pipeline.tasks.upload.s3 import UploadFilesToS3, _glob_root

NO_SUCH_KEY = botocore.exceptions.ClientError(
    {"Error": {"Code": "NoSuchKey"}}, "GetObjectAttributes"
)


def _client(remote_checksum: str | None = None) -> MagicMock:
    """A fake S3 client: object missing by default, or present with a checksum."""
    client = MagicMock()
    if remote_checksum is None:
        client.get_object_attributes.side_effect = NO_SUCH_KEY
    else:
        client.get_object_attributes.return_value = {
            "Checksum": {"ChecksumSHA256": remote_checksum}
        }
    return client


def _uploaded_keys(client: MagicMock) -> list[str]:
    return [call.kwargs["Key"] for call in client.upload_file.call_args_list]


def _run(task: UploadFilesToS3, client: MagicMock, asof: float = 1e12) -> None:
    with patch("src.pipeline.tasks.upload.s3.boto3.client", return_value=client):
        task.execute(asof_seconds=asof, lookback=None)


def test_glob_root_stops_at_first_wildcard() -> None:
    assert _glob_root("logs/2026/*/imu.mcap") == pathlib.Path("logs/2026")
    assert _glob_root("*.csv") == pathlib.Path(".")


def test_directory_source_preserves_structure_under_prefix(tmp_path: pathlib.Path) -> None:
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.csv").write_text("a")
    (tmp_path / "sub" / "b.csv").write_text("b")

    client = _client()
    _run(UploadFilesToS3(bucket="bkt", source=str(tmp_path), prefix="fleet/run1"), client)

    assert _uploaded_keys(client) == ["fleet/run1/a.csv", "fleet/run1/sub/b.csv"]
    assert all(call.kwargs["Bucket"] == "bkt" for call in client.upload_file.call_args_list)


def test_single_file_source_uses_basename(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "reduced.mcap"
    source.write_text("x")

    client = _client()
    _run(UploadFilesToS3(bucket="bkt", source=str(source)), client)

    assert _uploaded_keys(client) == ["reduced.mcap"]


def test_glob_source_relativizes_to_pattern_root(tmp_path: pathlib.Path) -> None:
    for run in ("run1", "run2"):
        (tmp_path / run).mkdir()
        (tmp_path / run / "out.csv").write_text(run)
    (tmp_path / "run1" / "ignored.txt").write_text("no")

    client = _client()
    _run(UploadFilesToS3(bucket="bkt", source=str(tmp_path / "*" / "*.csv")), client)

    assert _uploaded_keys(client) == ["run1/out.csv", "run2/out.csv"]


def test_skip_existing_with_matching_checksum(tmp_path: pathlib.Path) -> None:
    from src.artifacts import checksum_sha256

    source = tmp_path / "same.bin"
    source.write_bytes(b"identical")

    client = _client(remote_checksum=checksum_sha256(source))
    _run(UploadFilesToS3(bucket="bkt", source=str(source)), client)

    client.upload_file.assert_not_called()


def test_uploads_when_remote_checksum_differs(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "changed.bin"
    source.write_bytes(b"new content")

    client = _client(remote_checksum="c29tZXRoaW5nIGVsc2U=")
    _run(UploadFilesToS3(bucket="bkt", source=str(source)), client)

    assert _uploaded_keys(client) == ["changed.bin"]


def test_skip_existing_disabled_never_checks_remote(tmp_path: pathlib.Path) -> None:
    source = tmp_path / "always.bin"
    source.write_bytes(b"x")

    client = _client()
    _run(UploadFilesToS3(bucket="bkt", source=str(source), skip_existing=False), client)

    client.get_object_attributes.assert_not_called()
    assert _uploaded_keys(client) == ["always.bin"]


def test_filter_modified_at_uploads_only_files_in_window(tmp_path: pathlib.Path) -> None:
    import os

    fresh = tmp_path / "fresh.csv"
    stale = tmp_path / "stale.csv"
    fresh.write_text("f")
    stale.write_text("s")
    os.utime(fresh, (1000.0, 1000.0))
    os.utime(stale, (100.0, 100.0))

    task = UploadFilesToS3(bucket="bkt", source=str(tmp_path), filter_modified_at=True)
    client = _client()
    with patch("src.pipeline.tasks.upload.s3.boto3.client", return_value=client):
        task.execute(
            asof_seconds=1000.0, lookback=base.Lookback(last=60, unit=base.Unit.SECOND)
        )

    assert _uploaded_keys(client) == ["fresh.csv"]


def test_frame_lookback_rejected_with_filter(tmp_path: pathlib.Path) -> None:
    task = UploadFilesToS3(bucket="bkt", source=str(tmp_path), filter_modified_at=True)
    with (
        patch("src.pipeline.tasks.upload.s3.boto3.client", return_value=_client()),
        pytest.raises(ValueError, match="FRAME"),
    ):
        task.execute(asof_seconds=1.0, lookback=base.Lookback(last=5, unit=base.Unit.FRAME))


def test_empty_bucket_or_source_rejected() -> None:
    with pytest.raises(ValueError, match="bucket"):
        UploadFilesToS3(bucket="", source="x")
    with pytest.raises(ValueError, match="source"):
        UploadFilesToS3(bucket="b", source="")


def test_registry_discovers_upload_task() -> None:
    from src.pipeline import capabilities

    entries = capabilities.list_capabilities()
    upload = next(
        (e for e in entries if e["module"] == "src.pipeline.tasks.upload.s3"), None
    )
    assert upload is not None
    assert upload["kind"] == "task"
    param_names = {p["name"] for p in upload["parameters"]}
    assert {"bucket", "source", "prefix", "endpoint_url", "filter_modified_at"} <= param_names
