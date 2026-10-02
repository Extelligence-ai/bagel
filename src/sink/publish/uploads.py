"""Upload files through the paired fleet service without cloud access keys."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import time
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urlsplit

from src.sink.publish.identity import _atomic_write, load_identity

log = logging.getLogger(__name__)


class _RedactStorageAuthorization(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if (
            record.name == "httpx"
            and str(record.msg).startswith("HTTP Request:")
            and isinstance(record.args, tuple)
            and len(record.args) > 1
        ):
            url = urlsplit(str(record.args[1]))
            if url.query and any(
                name in url.query.lower() for name in ("signature=", "credential=")
            ):
                safe = url._replace(query="redacted", fragment="").geturl()
                record.args = (record.args[0], safe, *record.args[2:])
        return True


_storage_log_filter = _RedactStorageAuthorization()


def canonical(value: object) -> str:
    """Use the fleet upload v1 canonical JSON encoding."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


class UploadClient:
    """Sign narrowly scoped requests with the existing enrolled device key."""

    def __init__(self, directory: Path, owner: dict | None = None) -> None:
        """Use the current identity pointers, including renewed certificates."""
        self.directory = Path(directory)
        self.owner = owner
        # HTTPX logs full request URLs at INFO. The PUT query grants temporary
        # storage access and must never appear in ordinary robot logs.
        logging.getLogger("httpx").addFilter(_storage_log_filter)

    def request(self, operation: str, payload: dict) -> dict:
        """Authorize or confirm one immutable file through the enrollment origin."""
        import httpx
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.x509.oid import NameOID

        from src.sink.publish import require_fleet

        require_fleet()
        identity = load_identity(self.directory)
        expected = self.owner or {"tenant_id": identity.tenant, "robot_id": identity.robot_id}
        certificate = identity.cert_path.read_text()
        cert = x509.load_pem_x509_certificate(certificate.encode())
        cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        if cn != expected["tenant_id"] + "/" + expected["robot_id"]:
            raise ValueError("Upload belongs to another enrollment; retained locally.")
        # Check the actual certificate bytes used below, not just identity.yaml:
        # re-pairing can replace both files between reads. Renewal of the same
        # owner is allowed; a different owner never receives an old capture.
        self.owner = expected
        url = urlsplit(identity.enroll_url)
        if url.scheme != "https" or not url.hostname or url.username or url.password:
            raise ValueError("Fleet uploads require a verified HTTPS enrollment origin.")
        if operation not in ("presigned", "confirm"):
            raise ValueError("Unsupported fleet upload operation.")
        route = "/fleet/uploads/" + operation
        timestamp = int(time.time())
        message = (
            "fleet-upload-v1\nPOST\n" + route + "\n" + str(timestamp) + "\n" + canonical(payload)
        ).encode()
        key = serialization.load_pem_private_key(identity.key_path.read_bytes(), password=None)
        body = {
            "certificate": certificate,
            "timestamp": timestamp,
            "payload": payload,
            "signature": base64.b64encode(key.sign(message, ec.ECDSA(hashes.SHA256()))).decode(),
        }
        with httpx.Client(trust_env=False, timeout=30) as client:
            response = client.post("https://" + url.netloc + route, json=body)
            # Do not include signed URLs or request bodies in operator logs.
            if response.is_error:
                raise RuntimeError(f"Fleet upload {operation} refused: HTTP {response.status_code}")
            return response.json()

    def upload(self, path: Path, manifest: dict) -> dict:
        """Retry safely after lost PUT/confirm responses; S3 verifies the bytes."""
        import httpx

        grant = self.request("presigned", manifest)
        if not grant["confirmed"]:
            if urlsplit(grant["url"]).scheme != "https":
                raise ValueError("Fleet upload destination must use HTTPS.")
            with path.open("rb") as source, httpx.Client(trust_env=False, timeout=120) as client:
                response = client.put(
                    grant["url"],
                    content=source,
                    headers={**grant["headers"], "Content-Length": str(manifest["size_bytes"])},
                )
                # A lost successful response leaves an immutable object. Confirm
                # its checksum instead of overwriting or making a second clip.
                if response.is_error and response.status_code != HTTPStatus.PRECONDITION_FAILED:
                    raise RuntimeError(f"Fleet file transfer refused: HTTP {response.status_code}")
        return self.request("confirm", {"upload_id": grant["upload_id"]})


def manifest_for(path: Path, filename: str, provenance: dict) -> dict:
    """Bind content, relative path, and capture provenance into one manifest."""
    sha = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            sha.update(block)
    return {
        **provenance,
        "filename": filename,
        "sha256": sha.hexdigest(),
        "size_bytes": path.stat().st_size,
    }


def enqueue(root: Path, path: Path, provenance: dict, owner: dict) -> None:
    """Persist a capture receipt before reporting completion to the supervisor."""
    path = path.resolve()
    root = root.resolve()
    if not path.is_relative_to(root):
        raise ValueError("Upload artifact must remain inside its managed release.")
    manifest = manifest_for(path, path.name, provenance)
    key = hashlib.sha256(canonical(manifest).encode()).hexdigest()
    _atomic_write(
        root / "uploads" / (key + ".json"),
        canonical(
            {
                "path": str(path.relative_to(root)),
                "manifest": manifest,
                "owner": owner,
            }
        ).encode(),
        mode=0o600,
    )


def flush_pending(root: Path, directory: Path, limit: int = 8) -> None:
    """Retry durable capture receipts across worker stops and agent restarts."""
    identity = load_identity(directory)
    for receipt in sorted(root.glob("*/uploads/*.json"), key=lambda p: p.stat().st_mtime)[:limit]:
        try:
            record = json.loads(receipt.read_text())
            if record["owner"] != {"tenant_id": identity.tenant, "robot_id": identity.robot_id}:
                raise ValueError("Capture belongs to another enrollment; retained locally.")
            release = receipt.parent.parent.resolve()
            path = (release / record["path"]).resolve()
            if not path.is_relative_to(release):
                raise ValueError("Upload artifact escaped its managed release.")
            UploadClient(directory, record["owner"]).upload(path, record["manifest"])
            # Retain the receipt for local audit, but never resend confirmed work.
            receipt.rename(receipt.with_suffix(".confirmed"))
        except Exception as exc:
            # Rotate failed receipts behind work not yet attempted. A poisoned
            # capture or a previous enrollment must not starve later files.
            receipt.touch()
            log.warning("Capture upload pending (%s); receipt retained", type(exc).__name__)
