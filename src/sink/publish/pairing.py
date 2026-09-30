"""Pair a controller with an operator-approved fleet asset; no inbound networking."""

# CLI output deliberately contains only public identity facts.
# ruff: noqa: T201
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import pathlib
import platform
import re
import socket
import time
import urllib.error
import urllib.request
import uuid

from filelock import FileLock

from settings import settings
from src.sink.publish import identity, require_fleet


def _durable_write(target: pathlib.Path, data: bytes, *, mode: int | None = None) -> None:
    identity._atomic_write(target, data, mode=mode)
    with target.open("rb") as handle:
        os.fsync(handle.fileno())
    directory_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def discover(directory: pathlib.Path) -> dict:
    """Report local facts; a container never claims its VM metadata is robot hardware."""
    from src.sink.publish.heartbeat import bagel_version

    controller_file = directory / "controller-id"
    if not controller_file.exists():
        _durable_write(controller_file, ("ctl_" + uuid.uuid4().hex).encode(), mode=0o600)
    controller_id = controller_file.read_text().strip()
    if not re.fullmatch(r"ctl_[a-f0-9]{32}", controller_id):
        raise ValueError("Invalid persisted controller ID")
    container = pathlib.Path("/.dockerenv").exists() or pathlib.Path("/run/.containerenv").exists()

    def hardware(name: str) -> str | None:
        if container or platform.system() != "Linux":
            return None
        try:
            value = (pathlib.Path("/sys/class/dmi/id") / name).read_text().strip()
            if not value or value.lower() in {
                "none",
                "unknown",
                "default string",
                "to be filled by o.e.m.",
            }:
                return None
            return value[:200]
        except OSError:
            return None

    return {
        "controller_id": controller_id,
        "hostname": socket.gethostname()[:200],
        "platform": platform.system()[:200],
        "architecture": platform.machine()[:100],
        "bagel_version": bagel_version()[:100],
        "build_id": os.environ.get("BAGEL_BUILD_ID", "")[:200],
        "vcs_ref": os.environ.get("BAGEL_VCS_REF", "")[:100],
        "container": container,
        "hardware_manufacturer": hardware("sys_vendor"),
        "hardware_serial": hardware("product_serial"),
    }


def proof_message(request: dict) -> bytes:
    """Canonical, domain-separated controller proof shared with the fleet protocol."""
    value = {key: request[key] for key in ("pairing_id", "csr_pem", "discovery")}
    return (
        b"bagel.pairing.v1\n"
        + json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    )


def prepare(directory: pathlib.Path, url: str, code: str) -> dict:
    """Persist key, CSR and request together BEFORE the first outbound attempt."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    identity._validate_enroll_url(url)
    if not re.fullmatch(r"pair_[a-f0-9]{32}\.[a-f0-9]{64}", code):
        raise ValueError("Invalid pairing code")
    pending_path = directory / "pairing-pending.json"
    pairing_id, token = code.split(".")
    if pending_path.exists():
        pending = json.loads(pending_path.read_text())
        if pending["request"]["pairing_id"] == pairing_id:
            if pending["url"] != url or pending["request"]["token"] != token:
                raise ValueError("Pairing endpoint or code changed")
            return pending
        # An explicit new code starts a new attempt; old key retained for recovery.
        backup = directory / ("pairing-previous-" + pending["request"]["pairing_id"] + ".json")
        pending_path.replace(backup)
    key_pem, csr_pem = identity.generate_key_and_csr()
    private_key = serialization.load_pem_private_key(key_pem, password=None)
    request = {
        "pairing_id": pairing_id,
        "token": token,
        "csr_pem": csr_pem.decode(),
        "discovery": discover(directory),
    }
    request["proof"] = base64.b64encode(
        private_key.sign(proof_message(request), ec.ECDSA(hashes.SHA256()))
    ).decode()
    fingerprint = hashlib.sha256(
        private_key.public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    ).hexdigest()
    pending = {
        "url": url,
        "request": request,
        "private_key_pem": key_pem.decode(),
        "public_key_sha256": fingerprint,
    }
    _durable_write(pending_path, json.dumps(pending).encode(), mode=0o600)
    return pending


def exchange(pending: dict) -> dict:
    """Never expose request data or server error bodies in terminal/log output."""
    identity._validate_enroll_url(pending["url"])
    request = urllib.request.Request(  # noqa: S310 -- URL validated above
        pending["url"].rstrip("/") + "/v1/pairing",
        data=json.dumps(pending["request"]).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:  # noqa: S310
            return json.loads(response.read(65537))
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403, 409, 410, 422):
            raise ValueError(
                "Pairing denied or expired. Check the asset page before starting a new attempt."
            ) from None
        raise ConnectionError("Pairing service unavailable; retrying the same request") from None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        raise ConnectionError("Pairing service unavailable; retrying the same request") from None


def install(directory: pathlib.Path, pending: dict, response: dict) -> None:
    """Validate the issued identity and commit the identity document last."""
    import yaml
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    from cryptography.x509.oid import NameOID

    cert = x509.load_pem_x509_certificate(response["cert_pem"].encode())
    fingerprint = hashlib.sha256(
        cert.public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    ).hexdigest()
    if fingerprint != pending["public_key_sha256"]:
        raise ValueError("Issued certificate does not match this controller's key")
    expected_cn = response["tenant"] + "/" + response["robot_id"]
    if cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value != expected_cn:
        raise ValueError("Issued certificate identity mismatch")
    if not response["broker_url"].startswith("mqtts://"):
        raise ValueError("Pairing requires a TLS fleet broker")
    x509.load_pem_x509_certificate(response["ca_pem"].encode())
    if response.get("renew_url"):
        identity._validate_enroll_url(response["renew_url"])
    if identity.is_enrolled(directory):
        raise ValueError("Controller is already enrolled; refusing to replace its identity")
    _durable_write(directory / "robot.key", pending["private_key_pem"].encode(), mode=0o600)
    _durable_write(directory / "robot.crt", response["cert_pem"].encode())
    _durable_write(directory / "ca.crt", response["ca_pem"].encode())
    document = {key: response[key] for key in ("tenant", "robot_id", "broker_url", "expires_at")}
    document.update(
        enroll_url=pending["url"], key_file="robot.key", cert_file="robot.crt", ca_file="ca.crt"
    )
    if response.get("renew_url"):
        document["renew_url"] = response["renew_url"]
    _durable_write(directory / "identity.yaml", yaml.safe_dump(document).encode())
    (directory / "pairing-pending.json").unlink(missing_ok=True)


async def activate() -> bool:
    """Ask the running local server to reload its identity and existing stream rules."""
    from mcp import ClientSession
    from mcp.client.sse import sse_client

    try:
        async with sse_client(f"http://127.0.0.1:{settings.MCP_SERVER_PORT}/sse") as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("stream_live_topics", {})
                return not result.isError
    except Exception:
        return False


def main() -> int:  # noqa: C901 -- explicit CLI lifecycle and recovery branches
    """Run from the same environment and identity volume as the Bagel server."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url")
    parser.add_argument("--code")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    require_fleet()
    directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    try:
        with FileLock(str(directory / ".pairing.lock"), timeout=0):
            if identity.is_enrolled(directory):
                print(
                    "This controller is already enrolled. "
                    "Use the fleet controller replacement workflow to change it."
                )
                return 1
            if args.resume and not args.code and not args.url:
                pending = json.loads((directory / "pairing-pending.json").read_text())
            elif args.url and args.code and not args.resume:
                pending = prepare(directory, args.url, args.code)
            else:
                parser.error("use --url and --code, or --resume")
            print("Controller ID: " + pending["request"]["discovery"]["controller_id"], flush=True)
            print("Public key SHA-256: " + pending["public_key_sha256"], flush=True)
            print(
                "Waiting for operator confirmation on the asset page. "
                "Compare this fingerprint before confirming.",
                flush=True,
            )
            deadline = time.monotonic() + 900
            while time.monotonic() < deadline:
                try:
                    response = exchange(pending)
                except ConnectionError:
                    time.sleep(3)
                    continue
                if response.get("status") == "issued":
                    install(directory, pending, response)
                    print("Controller paired; certificate installed.", flush=True)
                    if asyncio.run(activate()):
                        print("Existing fleet streaming configuration reloaded.")
                    else:
                        print("Restart the Bagel server to load the new identity.")
                    return 0
                if response.get("status") != "awaiting_confirmation":
                    raise ValueError("Unexpected pairing status")
                time.sleep(3)
            print("Still pending. Run this module with --resume to retry using the same key.")
            return 1
    except KeyboardInterrupt:
        print("Pairing interrupted. Use --resume to continue with the saved key.")
        return 1
    except Exception:
        # Do not echo argparse input, key material, tokens, or remote response bodies.
        print(
            "Pairing could not finish. Check the asset page; "
            "use --resume after a network interruption."
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
