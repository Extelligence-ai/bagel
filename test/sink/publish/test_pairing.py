"""Pairing preserves keys across retries and never guesses physical robot identity."""

import base64
import json
import stat
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec

from src.sink.publish import identity, pairing
from test.sink.publish.test_identity import _build_ca_and_robot_cert

CODE = "pair_" + "a" * 32 + "." + "b" * 64


def test_preparation_preserves_key_proof_discovery_and_permissions(tmp_path: Path) -> None:
    pending = pairing.prepare(tmp_path, "https://fleet.example", CODE)
    again = pairing.prepare(tmp_path, "https://fleet.example", CODE)
    assert again == pending
    request = pending["request"]
    csr = x509.load_pem_x509_csr(request["csr_pem"].encode())
    csr.public_key().verify(
        base64.b64decode(request["proof"]),
        pairing.proof_message(request),
        ec.ECDSA(hashes.SHA256()),
    )
    assert csr.is_signature_valid
    assert stat.S_IMODE((tmp_path / "pairing-pending.json").stat().st_mode) == 0o600
    assert pairing.discover(tmp_path)["controller_id"] == request["discovery"]["controller_id"]
    if request["discovery"]["container"]:
        assert request["discovery"]["hardware_serial"] is None
    assert "serial_number" not in request["discovery"]
    with pytest.raises(ValueError, match="changed"):
        pairing.prepare(tmp_path, "https://another.example", CODE)


def response(pending: dict) -> dict:
    csr = x509.load_pem_x509_csr(pending["request"]["csr_pem"].encode())
    cert, ca = _build_ca_and_robot_cert("tenant/robot", csr.public_key())
    return {
        "cert_pem": cert.decode(),
        "ca_pem": ca.decode(),
        "tenant": "tenant",
        "robot_id": "robot",
        "broker_url": "mqtts://fleet.example:8883",
        "expires_at": "2099-01-01",
        "renew_url": "https://fleet.example:9443",
    }


def test_certificate_matches_persisted_key_and_installs_once(tmp_path: Path) -> None:
    pending = pairing.prepare(tmp_path, "https://fleet.example", CODE)
    issued = response(pending)
    pairing.install(tmp_path, pending, issued)
    enrolled = identity.load_identity(tmp_path)
    assert enrolled.tenant == "tenant"
    assert enrolled.key_path.read_text() == pending["private_key_pem"]
    assert not (tmp_path / "pairing-pending.json").exists()
    with pytest.raises(ValueError, match="already enrolled"):
        pairing.install(tmp_path, pending, issued)


def test_wrong_certificate_does_not_commit_identity(tmp_path: Path) -> None:
    pending = pairing.prepare(tmp_path, "https://fleet.example", CODE)
    other = tmp_path / "other"
    other.mkdir()
    different = pairing.prepare(other, "https://fleet.example", CODE)
    with pytest.raises(ValueError, match="does not match"):
        pairing.install(tmp_path, pending, response(different))
    assert not identity.is_enrolled(tmp_path)
    assert json.loads((tmp_path / "pairing-pending.json").read_text()) == pending
