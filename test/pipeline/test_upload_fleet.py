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


def test_certificate_owner_is_checked_again_on_every_signed_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import datetime, timedelta, timezone

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    import src.sink.publish

    monkeypatch.setattr(src.sink.publish, "require_fleet", lambda: None)
    key = ec.generate_private_key(ec.SECP256R1())
    key_path = tmp_path / "robot.key"
    cert_path = tmp_path / "robot.crt"
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    identity = SimpleNamespace(
        tenant="org",
        robot_id="robot",
        enroll_url="https://fleet.example/enroll",
        cert_path=cert_path,
        key_path=key_path,
    )
    monkeypatch.setattr(uploads, "load_identity", lambda _: identity)
    post = Mock(return_value=httpx.Response(200, json={"upload_id": "one"}))
    monkeypatch.setattr(httpx.Client, "post", post)
    client = uploads.UploadClient(tmp_path, {"tenant_id": "org", "robot_id": "robot"})
    for cn in ("org/robot", "other-org/other-robot"):
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(timezone.utc))
            .not_valid_after(datetime.now(timezone.utc) + timedelta(days=1))
            .sign(key, hashes.SHA256())
        )
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        if cn == "org/robot":
            client.request("presigned", {"filename": "clip.mcap"})
            envelope = post.call_args.kwargs["json"]
            import base64

            message = (
                "fleet-upload-v1\nPOST\n/fleet/uploads/presigned\n"
                + str(envelope["timestamp"])
                + "\n"
                + uploads.canonical(envelope["payload"])
            ).encode()
            cert.public_key().verify(
                base64.b64decode(envelope["signature"]), message, ec.ECDSA(hashes.SHA256())
            )
        else:
            # Even if identity.yaml is stale, use the same certificate snapshot
            # for ownership validation and the outgoing signed request.
            with pytest.raises(ValueError, match="another enrollment"):
                client.request("presigned", {"filename": "clip.mcap"})
    assert post.call_count == 1


def test_disabled_fleet_cannot_send_upload_proofs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from settings import settings
    from src.sink.publish import FleetDisabledError

    monkeypatch.setattr(settings, "FLEET_ENABLED", False)
    with pytest.raises(FleetDisabledError):
        uploads.UploadClient(tmp_path).request("presigned", {})


def test_signed_storage_url_is_redacted_from_real_httpx_request_logs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    path = tmp_path / "clip.mcap"
    path.write_bytes(b"clip")
    client = uploads.UploadClient(tmp_path)
    grant = {
        "upload_id": "receipt",
        "confirmed": False,
        "url": "https://storage/file?X-Amz-Credential=private-id&X-Amz-Signature=private-signature",
        "headers": {},
    }
    monkeypatch.setattr(client, "request", lambda *args: grant)
    original = httpx.Client
    transport = httpx.MockTransport(lambda request: httpx.Response(200))
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: original(transport=transport, **kwargs))
    with caplog.at_level(logging.INFO, logger="httpx"):
        client.upload(path, {"size_bytes": 4})
    assert "HTTP Request: PUT https://storage/file?redacted" in caplog.text
    assert "private-id" not in caplog.text
    assert "private-signature" not in caplog.text
