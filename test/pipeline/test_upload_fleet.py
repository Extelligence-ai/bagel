"""Paired fleet upload retries and managed capture durability."""

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from src.pipeline.tasks.upload.fleet import UploadFilesToFleet
from src.sink.publish import uploads


def test_pipeline_attaches_run_build_labels_without_aws_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "clip.mcap"
    path.write_bytes(b"clip")
    client = Mock()
    monkeypatch.setattr(UploadFilesToFleet, "_connect", lambda _: client)
    provenance = {
        "build_id": "v2",
        "anomaly_labels": [{"label": "stall", "t_start": 1, "t_end": 2}],
    }
    UploadFilesToFleet(str(path), "run-1", "real", provenance).execute(0, None)
    manifest = client.upload.call_args.args[1]
    assert manifest == {
        **provenance,
        "run_id": "run-1",
        "origin": "real",
        "filename": "clip.mcap",
        "size_bytes": 4,
        "sha256": hashlib.sha256(b"clip").hexdigest(),
    }


def test_lost_put_response_confirms_existing_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "clip.mcap"
    path.write_bytes(b"clip")
    client = uploads.UploadClient(tmp_path)
    requests = []

    def request(operation: str, payload: dict) -> dict:
        requests.append((operation, payload))
        return {
            "upload_id": "receipt",
            "confirmed": False,
            "url": "https://storage/file",
            "headers": {"x-amz-checksum-sha256": "checksum"},
        }

    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(httpx.Client, "put", lambda *a, **kw: httpx.Response(412))
    client.upload(path, {"size_bytes": 4})
    assert requests[-1] == ("confirm", {"upload_id": "receipt"})


def test_pending_survives_failure_restart_and_never_moves_to_other_org(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = tmp_path / "releases" / "revision"
    release.mkdir(parents=True)
    artifact = release / "clip.mcap"
    artifact.write_bytes(b"clip")
    owner = {"tenant_id": "org-1", "robot_id": "robot-1"}
    uploads.enqueue(release, artifact, {"run_id": "run-1", "origin": "real"}, owner)
    receipts = list(release.glob("uploads/*.json"))
    assert len(receipts) == 1
    assert json.loads(receipts[0].read_text())["owner"] == owner
    identity = SimpleNamespace(tenant="other-org", robot_id="robot-1")
    monkeypatch.setattr(uploads, "load_identity", lambda _: identity)
    send = Mock(side_effect=RuntimeError("offline"))
    monkeypatch.setattr(uploads.UploadClient, "upload", send)
    uploads.flush_pending(tmp_path / "releases", tmp_path / "identity")
    send.assert_not_called()
    identity.tenant = "org-1"
    uploads.flush_pending(tmp_path / "releases", tmp_path / "identity")
    assert receipts[0].exists()
    send.side_effect = None
    uploads.flush_pending(tmp_path / "releases", tmp_path / "identity")
    assert not receipts[0].exists()
    assert receipts[0].with_suffix(".confirmed").exists()
    uploads.flush_pending(tmp_path / "releases", tmp_path / "identity")
    assert send.call_count == 2
    assert artifact.exists()


def test_manifest_changes_with_content_and_retains_exact_provenance(tmp_path: Path) -> None:
    path = tmp_path / "clip.mcap"
    path.write_bytes(b"one")
    provenance = {"run_id": "run", "origin": "sim", "t_start": 1.25, "t_end": 3.5}
    first = uploads.manifest_for(path, path.name, provenance)
    path.write_bytes(b"two")
    second = uploads.manifest_for(path, path.name, provenance)
    assert first["sha256"] != second["sha256"]
    assert all(second[k] == v for k, v in provenance.items())
