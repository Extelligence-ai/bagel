"""Enrollment client: keygen, CSR, enroll/store/load identity (spec §6).

The fake enroll server is a real stdlib http.server, started on 127.0.0.1:0 in
a background thread, backed by a throwaway CA + signed robot cert built with
cryptography right here in the test -- so the PEMs written to disk by
``enroll()`` are real, parseable certs, not string fixtures.
"""

import dataclasses
import datetime
import http.server
import importlib
import json
import logging
import pathlib
import socket
import ssl
import stat
import sys
import threading
import time

import pytest
import yaml

from settings import settings
from src.sink.publish import EnrollmentError, FleetNotEnrolledError
from src.sink.publish import identity as identity_mod

VALID_TOKEN = "valid-token"  # noqa: S105 -- test fixture token, not a real secret
UNKNOWN_TOKEN = "unknown-token"  # noqa: S105
USED_TOKEN = "used-token"  # noqa: S105
MALFORMED_TOKEN = "malformed-token"  # noqa: S105
SERVER_ERROR_TOKEN = "server-error-token"  # noqa: S105
# A 500 whose body echoes this token verbatim (debug-mode server / reflecting
# proxy / malicious endpoint) -- the redaction regression test's token.
ECHOING_ERROR_TOKEN = "echoing-error-token-secret-xyz"  # noqa: S105


def _build_ca_and_robot_cert(common_name: str, robot_public_key: object) -> tuple[bytes, bytes]:
    """Build a throwaway self-signed CA and a robot cert it signs, both as PEM.

    Returns (robot_cert_pem, ca_cert_pem).
    """
    import datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test fleet CA")])
    now = datetime.datetime.now(datetime.UTC)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    robot_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    robot_cert = (
        x509.CertificateBuilder()
        .subject_name(robot_name)
        .issuer_name(ca_name)
        .public_key(robot_public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=90))
        .sign(ca_key, hashes.SHA256())
    )

    return (
        robot_cert.public_bytes(serialization.Encoding.PEM),
        ca_cert.public_bytes(serialization.Encoding.PEM),
    )


class _FakeEnrollHandler(http.server.BaseHTTPRequestHandler):
    """POST /v1/enroll -- canned responses keyed off the request's token."""

    def log_message(self, fmt: str, *args: object) -> None:
        pass  # Silence the default stderr access log during tests.

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw)
        token = payload.get("token")
        csr_pem = payload["csr_pem"].encode()

        if token == UNKNOWN_TOKEN:
            self._respond(401, b"unknown token")
            return
        if token == USED_TOKEN:
            self._respond(410, b"token already used")
            return
        if token == SERVER_ERROR_TOKEN:
            self._respond(500, b"internal error")
            return
        if token == ECHOING_ERROR_TOKEN:
            # Simulates a debug-mode server (or a reflecting proxy/WAF) that
            # echoes the failed request back in its error body, token included.
            self._respond(500, f"internal error, request was: {raw.decode()}".encode())
            return
        if token == MALFORMED_TOKEN:
            self._respond(200, json.dumps({"cert_pem": "not enough fields"}).encode())
            return

        from cryptography import x509

        csr = x509.load_pem_x509_csr(csr_pem)
        robot_cert_pem, ca_cert_pem = _build_ca_and_robot_cert(
            "forced-cn-by-server", csr.public_key()
        )
        response_fields = {
            "cert_pem": robot_cert_pem.decode(),
            "ca_pem": ca_cert_pem.decode(),
            "broker_url": "mqtts://fleet.example.com:8883",
            "tenant": "acme",
            "robot_id": "robot-42",
            "expires_at": "2027-01-01T00:00:00Z",
        }
        # A "renew-url:<base>" token asks the fake server to additionally
        # return that as `renew_url` -- proves enroll() stores an OPTIONAL,
        # server-supplied renew endpoint distinct from its own base (see
        # TestRenewUrl, which points this at a fake renew server on a
        # different port than the enroll server, mirroring the real
        # split-host topology).
        if token is not None and token.startswith("renew-url:"):
            response_fields["renew_url"] = token[len("renew-url:") :]
        body = json.dumps(response_fields).encode()
        self._respond(200, body)

    def _respond(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def fake_server() -> str:
    """Start the fake enroll server on an ephemeral port; yield its base URL."""
    server = http.server.HTTPServer(("127.0.0.1", 0), _FakeEnrollHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


class _FakeRenewHandler(http.server.BaseHTTPRequestHandler):
    """POST /v1/renew -- canned responses keyed off the CSR's common name.

    renew()'s request carries no token to key a scenario off (unlike
    enroll's fake server), so these tests instead set `identity.robot_id` to
    a scenario name before calling `renew()` -- `renew()` uses `robot_id` as
    the CSR's advisory common name, so this handler reads it back off the
    CSR it receives.
    """

    def log_message(self, fmt: str, *args: object) -> None:
        pass  # Silence the default stderr access log during tests.

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw)
        csr_pem = payload["csr_pem"].encode()

        from cryptography import x509
        from cryptography.x509.oid import NameOID

        csr = x509.load_pem_x509_csr(csr_pem)
        scenario = csr.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value

        if scenario == "renew-501":
            self._respond(501, b"renewal not enabled on this deployment")
            return
        if scenario == "renew-404":
            self._respond(404, b"not found")
            return
        if scenario == "renew-500":
            self._respond(500, b"internal error")
            return
        if scenario == "renew-malformed":
            self._respond(200, json.dumps({"cert_pem": "only-this-field"}).encode())
            return
        if scenario == "renew-not-json":
            self._respond(200, b"not json at all")
            return

        robot_cert_pem, ca_cert_pem = _build_ca_and_robot_cert("renewed-cn", csr.public_key())
        body = json.dumps(
            {
                "cert_pem": robot_cert_pem.decode(),
                "ca_pem": ca_cert_pem.decode(),
                "expires_at": "2028-01-01T00:00:00Z",
            }
        ).encode()
        self._respond(200, body)

    def _respond(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def fake_renew_server() -> str:
    """Start the fake renew server on an ephemeral port; yield its base URL."""
    server = http.server.HTTPServer(("127.0.0.1", 0), _FakeRenewHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _identity_for_renew(
    fake_server: str, fake_renew_server: str, tmp_path: object, scenario: str
) -> "identity_mod.Identity":
    """Enroll for real key/cert/ca files, then point at the renew server with a scenario CN.

    `_FakeRenewHandler` reads `scenario` back off the CSR's common name (see
    its docstring) to pick a canned response.
    """
    directory = tmp_path / "identity"
    enrolled = identity_mod.enroll(VALID_TOKEN, fake_server, directory)
    return dataclasses.replace(enrolled, robot_id=scenario, enroll_url=fake_renew_server)


class TestGenerateKeyAndCsr:
    def test_returns_parseable_ec_key_and_csr(self) -> None:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.x509 import load_pem_x509_csr
        from cryptography.x509.oid import NameOID

        key_pem, csr_pem = identity_mod.generate_key_and_csr(common_name="my-robot")

        private_key = serialization.load_pem_private_key(key_pem, password=None)
        assert isinstance(private_key.curve, ec.SECP256R1)

        csr = load_pem_x509_csr(csr_pem)
        assert csr.is_signature_valid
        cn = csr.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        assert cn[0].value == "my-robot"

    def test_default_common_name_is_robot(self) -> None:
        from cryptography.x509 import load_pem_x509_csr
        from cryptography.x509.oid import NameOID

        _, csr_pem = identity_mod.generate_key_and_csr()
        csr = load_pem_x509_csr(csr_pem)
        cn = csr.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        assert cn[0].value == "robot"


class TestEnrollHappyPath:
    def test_enroll_writes_files_and_returns_identity(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        result = identity_mod.enroll(VALID_TOKEN, fake_server, directory)

        assert result.tenant == "acme"
        assert result.robot_id == "robot-42"
        assert result.robot == "acme/robot-42"
        assert result.broker_url == "mqtts://fleet.example.com:8883"
        assert result.enroll_url == fake_server
        assert result.expires_at == "2027-01-01T00:00:00Z"
        assert result.key_path == directory / "robot.key"
        assert result.cert_path == directory / "robot.crt"
        assert result.ca_path == directory / "ca.crt"

        assert result.key_path.is_file()
        assert result.cert_path.is_file()
        assert result.ca_path.is_file()
        assert (directory / "identity.yaml").is_file()

    def test_key_file_mode_is_0600(self, fake_server: str, tmp_path: object) -> None:
        directory = tmp_path / "identity"
        result = identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        mode = stat.S_IMODE(result.key_path.stat().st_mode)
        assert mode == 0o600

    def test_identity_yaml_round_trips(self, fake_server: str, tmp_path: object) -> None:
        directory = tmp_path / "identity"
        result = identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        on_disk = yaml.safe_load((directory / "identity.yaml").read_text())
        assert on_disk == {
            "tenant": "acme",
            "robot_id": "robot-42",
            "broker_url": "mqtts://fleet.example.com:8883",
            "enroll_url": fake_server,
            "expires_at": "2027-01-01T00:00:00Z",
            # Pointer fields (see the module docstring): enroll() always
            # writes them explicitly, equal to load_identity's own defaults.
            "key_file": "robot.key",
            "cert_file": "robot.crt",
            "ca_file": "ca.crt",
        }
        assert result.tenant == on_disk["tenant"]

    def test_stored_cert_and_key_are_a_matching_real_pair(
        self, fake_server: str, tmp_path: object
    ) -> None:
        from cryptography import x509
        from cryptography.hazmat.primitives import serialization
        from cryptography.x509.oid import NameOID

        directory = tmp_path / "identity"
        result = identity_mod.enroll(VALID_TOKEN, fake_server, directory)

        private_key = serialization.load_pem_private_key(
            result.key_path.read_bytes(), password=None
        )
        cert = x509.load_pem_x509_certificate(result.cert_path.read_bytes())
        ca_cert = x509.load_pem_x509_certificate(result.ca_path.read_bytes())

        assert cert.public_key().public_numbers() == private_key.public_key().public_numbers()
        assert cert.issuer == ca_cert.subject
        cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        assert cn[0].value == "forced-cn-by-server"


class TestEnrollErrors:
    def test_unknown_token_raises_401_with_no_files_written(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        with pytest.raises(EnrollmentError) as excinfo:
            identity_mod.enroll(UNKNOWN_TOKEN, fake_server, directory)
        assert excinfo.value.status == 401
        assert not directory.exists() or not any(directory.iterdir())

    def test_used_token_raises_410_with_no_files_written(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        with pytest.raises(EnrollmentError) as excinfo:
            identity_mod.enroll(USED_TOKEN, fake_server, directory)
        assert excinfo.value.status == 410
        assert not directory.exists() or not any(directory.iterdir())

    def test_server_error_raises_with_status(self, fake_server: str, tmp_path: object) -> None:
        with pytest.raises(EnrollmentError) as excinfo:
            identity_mod.enroll(SERVER_ERROR_TOKEN, fake_server, tmp_path / "identity")
        assert excinfo.value.status == 500

    def test_malformed_200_response_raises_200(self, fake_server: str, tmp_path: object) -> None:
        with pytest.raises(EnrollmentError) as excinfo:
            identity_mod.enroll(MALFORMED_TOKEN, fake_server, tmp_path / "identity")
        assert excinfo.value.status == 200
        assert "malformed response" in excinfo.value.reason

    def test_connection_refused_raises_status_zero(self, tmp_path: object) -> None:
        # Reserve then release a port so nothing is listening on it.
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        with pytest.raises(EnrollmentError) as excinfo:
            identity_mod.enroll(VALID_TOKEN, f"http://127.0.0.1:{port}", tmp_path / "identity")
        assert excinfo.value.status == 0

    def test_bad_scheme_raises_value_error(self, tmp_path: object) -> None:
        with pytest.raises(ValueError, match="http"):
            identity_mod.enroll(VALID_TOKEN, "ftp://example.com", tmp_path / "identity")


class TestTokenHygiene:
    def test_token_never_logged_on_success(
        self, fake_server: str, tmp_path: object, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            identity_mod.enroll(VALID_TOKEN, fake_server, tmp_path / "identity")
        for record in caplog.records:
            assert VALID_TOKEN not in record.getMessage()

    def test_token_never_logged_or_embedded_in_error_on_failure(
        self, fake_server: str, tmp_path: object, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(EnrollmentError) as excinfo:
                identity_mod.enroll(UNKNOWN_TOKEN, fake_server, tmp_path / "identity")
        assert UNKNOWN_TOKEN not in str(excinfo.value)
        assert UNKNOWN_TOKEN not in excinfo.value.reason
        for record in caplog.records:
            assert UNKNOWN_TOKEN not in record.getMessage()

    def test_token_is_redacted_from_an_echoing_server_error_body(
        self, fake_server: str, tmp_path: object, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The 401/410 branches use hardcoded reason strings, so they can't leak
        # the token even by accident -- this covers the general (other-status)
        # branch, which forwards a snippet of the server's actual body.
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(EnrollmentError) as excinfo:
                identity_mod.enroll(ECHOING_ERROR_TOKEN, fake_server, tmp_path / "identity")
        assert excinfo.value.status == 500
        assert ECHOING_ERROR_TOKEN not in excinfo.value.reason
        assert ECHOING_ERROR_TOKEN not in str(excinfo.value)
        assert "***REDACTED***" in excinfo.value.reason
        for record in caplog.records:
            assert ECHOING_ERROR_TOKEN not in record.getMessage()


class TestLoadIdentity:
    def test_load_after_enroll_returns_equal_identity(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        enrolled = identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        loaded = identity_mod.load_identity(directory)
        assert loaded == enrolled

    def test_load_on_empty_dir_raises_not_enrolled(self, tmp_path: object) -> None:
        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(tmp_path / "nope")

    def test_load_with_missing_cert_file_raises_not_enrolled(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        (directory / "robot.crt").unlink()
        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(directory)

    def test_load_with_corrupt_yaml_raises_not_enrolled(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        (directory / "identity.yaml").write_text("not: valid: yaml: [")
        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(directory)

    def test_loader_tolerates_unknown_extra_keys(self, fake_server: str, tmp_path: object) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["last_renewal_attempt_at"] = "2026-06-01T00:00:00Z"
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))
        loaded = identity_mod.load_identity(directory)
        assert loaded.tenant == "acme"

    def test_legacy_identity_yaml_without_pointer_fields_still_loads(
        self, fake_server: str, tmp_path: object
    ) -> None:
        # Simulates an identity.yaml written before the key_file/cert_file/
        # ca_file pointer scheme existed: enroll() always writes them now
        # (see TestEnrollHappyPath.test_identity_yaml_round_trips), so this
        # strips them back out to exercise load_identity's fallback defaults.
        directory = tmp_path / "identity"
        enrolled = identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        for key in ("key_file", "cert_file", "ca_file", "renew_url"):
            doc.pop(key, None)
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        loaded = identity_mod.load_identity(directory)

        assert loaded.key_path == directory / "robot.key"
        assert loaded.cert_path == directory / "robot.crt"
        assert loaded.ca_path == directory / "ca.crt"
        assert loaded.renew_url is None
        assert loaded.tenant == enrolled.tenant
        assert loaded.expires_at == enrolled.expires_at

    def test_legacy_identity_without_pointer_fields_is_still_renewable(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        # A legacy identity must not just load -- it must also be usable as
        # the input to a real renewal (mTLS context built from its resolved
        # fixed-name paths, then rotated onto the new pointer scheme).
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        for key in ("key_file", "cert_file", "ca_file"):
            doc.pop(key, None)
        doc["enroll_url"] = fake_renew_server
        doc["robot_id"] = "renew-ok"
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))
        legacy = identity_mod.load_identity(directory)

        result = identity_mod.renew(legacy)

        assert result is not None
        assert result.key_path != legacy.key_path  # rotated onto the pointer scheme
        assert not legacy.key_path.exists()  # old fixed-name file unlinked


class TestLoadIdentityPointerFieldTypeValidation:
    """IMPORTANT fix: a hand-edited identity.yaml with a null pointer field must
    never raise a raw TypeError -- that would brick server boot through
    ``maybe_enroll_on_first_boot`` (whose contract is "never raises").
    ``load_identity`` used to do ``directory / data.get("key_file", "robot.key")``,
    where ``.get``'s default only applies when the key is ABSENT -- an explicit
    ``key_file: null`` still resolves to ``None`` and blows up the ``/`` operator.
    """

    def test_null_key_file_is_treated_as_not_enrolled(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["key_file"] = None
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(directory)
        assert identity_mod.is_enrolled(directory) is False

    def test_null_cert_file_is_treated_as_not_enrolled(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["cert_file"] = None
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(directory)

    def test_null_ca_file_is_treated_as_not_enrolled(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["ca_file"] = None
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(directory)

    def test_non_string_key_file_is_treated_as_not_enrolled(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["key_file"] = 42
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(directory)

    def test_unquoted_expires_at_timestamp_is_treated_as_not_enrolled(
        self, fake_server: str, tmp_path: object
    ) -> None:
        # A hand-edited identity.yaml with an unquoted timestamp: YAML's
        # implicit resolver parses it as a datetime object, not a string --
        # should_attempt_renewal's `expires_at.replace("Z", ...)` would then
        # blow up (or silently misbehave) instead of being treated as the
        # corrupt/not-enrolled document it actually is.
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        text = (directory / "identity.yaml").read_text()
        text = text.replace(
            "expires_at: '2027-01-01T00:00:00Z'", "expires_at: 2027-01-01T00:00:00Z"
        )
        (directory / "identity.yaml").write_text(text)
        # Sanity check the edit actually triggers YAML's auto-datetime edge.
        parsed = yaml.safe_load(text)
        assert isinstance(parsed["expires_at"], datetime.datetime)

        with pytest.raises(FleetNotEnrolledError):
            identity_mod.load_identity(directory)

    def test_legacy_absent_pointer_fields_still_default_correctly(
        self, fake_server: str, tmp_path: object
    ) -> None:
        # Regression guard: the null-vs-absent distinction must not break the
        # existing legacy-loading behavior (see
        # TestLoadIdentity.test_legacy_identity_yaml_without_pointer_fields_still_loads) --
        # an ABSENT field still means "use the historical fixed name", only an
        # EXPLICIT null is corrupt.
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        for key in ("key_file", "cert_file", "ca_file"):
            doc.pop(key, None)
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        loaded = identity_mod.load_identity(directory)

        assert loaded.key_path == directory / "robot.key"
        assert loaded.cert_path == directory / "robot.crt"
        assert loaded.ca_path == directory / "ca.crt"


class TestMaybeEnrollOnFirstBootKillSwitch:
    """Codex review: FLEET_ENABLED=0 must make maybe_enroll_on_first_boot() a
    no-op even when a one-time token+URL are configured -- the kill switch's
    documented "makes the subsystem inert" contract previously had no guard
    in this call path (server.py's __main__ calls it unconditionally).
    """

    def test_fleet_disabled_skips_enrollment_even_with_token_and_url_set(
        self, tmp_path: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        directory = tmp_path / "identity"
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)
        monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", "some-token")
        monkeypatch.setattr(settings, "FLEET_ENROLL_URL", "https://enroll.example.com")
        monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(directory))

        calls = []

        def spy_enroll(*a: object, **kw: object) -> None:
            calls.append((a, kw))
            raise AssertionError("enroll() must not be called when FLEET_ENABLED=0")

        monkeypatch.setattr(identity_mod, "enroll", spy_enroll)

        identity_mod.maybe_enroll_on_first_boot()  # must not raise, must not enroll

        assert calls == []  # enroll() itself was never invoked -- not just its raise swallowed
        assert identity_mod.is_enrolled(directory) is False

    def test_fleet_disabled_logs_skip_reason(
        self,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        directory = tmp_path / "identity"
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)
        monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", "some-token")
        monkeypatch.setattr(settings, "FLEET_ENROLL_URL", "https://enroll.example.com")
        monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(directory))

        with caplog.at_level(logging.DEBUG):
            identity_mod.maybe_enroll_on_first_boot()

        assert any(
            "FLEET_ENABLED" in record.getMessage() for record in caplog.records
        )


class TestMaybeEnrollOnFirstBootNeverRaisesOnCorruptIdentity:
    def test_survives_a_null_key_file_in_an_existing_identity_yaml(
        self, fake_server: str, tmp_path: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["key_file"] = None
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", VALID_TOKEN)
        monkeypatch.setattr(settings, "FLEET_ENROLL_URL", fake_server)
        monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(directory))

        identity_mod.maybe_enroll_on_first_boot()  # must not raise

        # is_enrolled() saw the corrupt doc as not-enrolled, so this re-enrolled.
        assert identity_mod.is_enrolled(directory) is True


class TestLastRenewalAttemptAtPersistence:
    """IMPORTANT fix: `last_renewal_attempt_at` must survive a process restart.

    `_write_identity_yaml` already persists it, but `load_identity` never
    read it back -- a crashlooping robot inside the 30-day renewal window
    would re-fire a renewal POST on every restart, ignoring the daily rate
    limit entirely.
    """

    def test_load_identity_reads_last_renewal_attempt_at(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["last_renewal_attempt_at"] = 1735689600.5
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        loaded = identity_mod.load_identity(directory)

        assert loaded.last_renewal_attempt_at == 1735689600.5

    def test_load_identity_defaults_to_none_when_field_absent(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)  # never writes the field

        loaded = identity_mod.load_identity(directory)

        assert loaded.last_renewal_attempt_at is None

    def test_load_identity_tolerates_a_wrong_typed_value(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        doc["last_renewal_attempt_at"] = "not-a-number"
        (directory / "identity.yaml").write_text(yaml.safe_dump(doc))

        loaded = identity_mod.load_identity(directory)

        assert loaded.last_renewal_attempt_at is None


class TestIdentityDirectoryPermissions:
    """MINOR fix: a freshly created identity directory should be 0700, not the
    default (umask-dependent, typically 0755) mkdir mode -- it holds a
    private key plus mTLS material.
    """

    def test_freshly_created_directory_is_mode_0700(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        assert not directory.exists()  # sanity: enroll() must create it

        identity_mod.enroll(VALID_TOKEN, fake_server, directory)

        mode = stat.S_IMODE(directory.stat().st_mode)
        assert mode == 0o700


class TestIsEnrolled:
    def test_true_after_enroll(self, fake_server: str, tmp_path: object) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        assert identity_mod.is_enrolled(directory) is True

    def test_false_on_empty_dir(self, tmp_path: object) -> None:
        assert identity_mod.is_enrolled(tmp_path / "nope") is False


class TestRenewUrl:
    """enroll_url and renew_url are separate BASES -- enroll (path-routed HTTPS)
    and renew (an mTLS listener on the broker) commonly live on different
    hosts entirely. See the module docstring.
    """

    def test_enroll_stores_renew_url_when_response_carries_one(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        token = "renew-url:https://renew.example.com:9443"  # noqa: S105 -- test token, not a secret
        result = identity_mod.enroll(token, fake_server, directory)

        assert result.renew_url == "https://renew.example.com:9443"
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        assert doc["renew_url"] == "https://renew.example.com:9443"

    def test_enroll_leaves_renew_url_absent_when_response_omits_it(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        result = identity_mod.enroll(VALID_TOKEN, fake_server, directory)

        assert result.renew_url is None
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        assert "renew_url" not in doc

    def test_load_identity_round_trips_renew_url(self, fake_server: str, tmp_path: object) -> None:
        directory = tmp_path / "identity"
        token = "renew-url:https://renew.example.com:9443"  # noqa: S105
        identity_mod.enroll(token, fake_server, directory)

        loaded = identity_mod.load_identity(directory)

        assert loaded.renew_url == "https://renew.example.com:9443"

    def test_load_identity_defaults_renew_url_to_none_when_field_absent(
        self, fake_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)

        loaded = identity_mod.load_identity(directory)

        assert loaded.renew_url is None

    def test_renew_targets_renew_url_not_enroll_url_when_present(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        # The split-host topology proof: the enroll server and the renew
        # server are two independent fake HTTP servers on two independent
        # ephemeral ports. enroll_url is deliberately left pointing at a
        # dead port (nothing listens there) -- if renew() derived its
        # target from enroll_url instead of renew_url, this would fail with
        # a connection-refused EnrollmentError-shaped None, not a success.
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        dead_port = sock.getsockname()[1]
        sock.close()
        dead_enroll_url = f"http://127.0.0.1:{dead_port}"

        directory = tmp_path / "identity"
        token = f"renew-url:{fake_renew_server}"
        enrolled = identity_mod.enroll(token, fake_server, directory)
        identity = dataclasses.replace(enrolled, robot_id="renew-ok", enroll_url=dead_enroll_url)
        assert identity.renew_url == fake_renew_server

        result = identity_mod.renew(identity)

        assert result is not None  # reached the renew server, not the dead enroll_url

    def test_renew_falls_back_to_enroll_url_when_renew_url_absent(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        # Today's behavior, preserved: no renew_url on the identity means
        # renew() must still target enroll_url/v1/renew.
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        assert identity.renew_url is None

        result = identity_mod.renew(identity)

        assert result is not None

    def test_renew_url_survives_a_renewal_rewrite(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        directory = tmp_path / "identity"
        token = f"renew-url:{fake_renew_server}"
        enrolled = identity_mod.enroll(token, fake_server, directory)
        identity = dataclasses.replace(enrolled, robot_id="renew-ok")

        result = identity_mod.renew(identity)

        assert result is not None
        assert result.renew_url == fake_renew_server
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        assert doc["renew_url"] == fake_renew_server


class TestEnrollUrlSchemeGate:
    """Codex review (3909074259): enroll() only checked that enroll_url was
    http(s) at all -- any nonlocal http:// host was accepted, POSTing the
    one-time token in cleartext. Now requires https:// unless the host is
    loopback/private (`connect._is_local_or_private`, reused not duplicated)
    or `settings.FLEET_DEV_INSECURE` is set -- mirrors the broker's own
    plaintext-transport policy exactly.
    """

    def test_nonlocal_http_raises_typed_insecure_error(
        self, tmp_path: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_getaddrinfo(host: str, *_a: object, **_kw: object) -> list:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

        request_calls: list[str] = []
        monkeypatch.setattr(
            identity_mod.urllib.request,
            "Request",
            lambda url, *a, **kw: request_calls.append(url),
        )

        with pytest.raises(EnrollmentError) as excinfo:
            identity_mod.enroll(VALID_TOKEN, "http://enroll.example.com", tmp_path / "identity")

        assert excinfo.value.status == 0
        assert excinfo.value.reason == "insecure enroll_url"
        assert request_calls == []  # rejected before any network call

    def test_localhost_http_is_ok(self, fake_server: str, tmp_path: object) -> None:
        # fake_server binds 127.0.0.1 -- a loopback literal -- which the gate
        # must allow over plain http with no FLEET_DEV_INSECURE needed.
        assert fake_server.startswith("http://127.0.0.1")
        result = identity_mod.enroll(VALID_TOKEN, fake_server, tmp_path / "identity")
        assert result.tenant == "acme"

    def test_https_is_always_ok(self, tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_getaddrinfo(host: str, *_a: object, **_kw: object) -> list:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

        # No real server needed -- prove the gate itself doesn't reject this
        # URL by checking the shared helper directly, since a real https
        # POST needs a TLS listener out of scope here (covered by
        # test_mqtt_integration.py-style real-TLS tests elsewhere).
        assert identity_mod._url_passes_scheme_gate("https://enroll.example.com") is True

    def test_dev_insecure_allows_nonlocal_http(
        self, tmp_path: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_getaddrinfo(host: str, *_a: object, **_kw: object) -> list:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)

        assert identity_mod._url_passes_scheme_gate("http://enroll.example.com") is True


class TestRenewUrlSchemeGate:
    """Same gate applied to the renew URL at use time -- renew() never
    raises (per its contract), so an insecure target is treated the same as
    the existing "not http(s)" branch: logged, the attempt recorded, None
    returned.
    """

    def test_insecure_renew_target_is_skipped_not_attempted(
        self, fake_server: str, tmp_path: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Enroll for real (against fake_server's real loopback listener)
        # BEFORE faking getaddrinfo -- urlopen's own connection setup also
        # goes through socket.getaddrinfo, so faking it globally during the
        # enroll() call would break that real connection too, not just the
        # _is_local_or_private check this test cares about.
        directory = tmp_path / "identity"
        enrolled = identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        identity = dataclasses.replace(
            enrolled, robot_id="renew-ok", enroll_url="http://renew.example.com"
        )

        def fake_getaddrinfo(host: str, *_a: object, **_kw: object) -> list:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

        request_calls: list[str] = []
        real_request = identity_mod.urllib.request.Request

        def spy_request(url: str, *a: object, **kw: object) -> object:
            request_calls.append(url)
            return real_request(url, *a, **kw)

        monkeypatch.setattr(identity_mod.urllib.request, "Request", spy_request)

        result = identity_mod.renew(identity)

        assert result is None
        assert request_calls == []  # never attempted the POST
        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        assert "last_renewal_attempt_at" in doc  # attempt still recorded


class TestBaseUrlTrailingSlash:
    """MINOR fix: a trailing slash on the configured base URL must not produce
    a double slash before the appended path (``FLEET_ENROLL_URL=https://x/``
    must hit ``.../v1/enroll``, not ``...//v1/enroll``). The stored BASE
    value itself (``identity.yaml``'s ``enroll_url``) is untouched --
    trailing slash and all -- only the REQUEST url is stripped.
    """

    def test_enroll_strips_trailing_slash_before_appending_the_path(
        self, fake_server: str, tmp_path: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured_urls: list[str] = []
        real_request = identity_mod.urllib.request.Request

        def spy_request(url: str, *args: object, **kwargs: object) -> object:
            captured_urls.append(url)
            return real_request(url, *args, **kwargs)

        monkeypatch.setattr(identity_mod.urllib.request, "Request", spy_request)

        directory = tmp_path / "identity"
        result = identity_mod.enroll(VALID_TOKEN, f"{fake_server}/", directory)

        assert captured_urls == [f"{fake_server}/v1/enroll"]  # no double slash
        # BASE semantics unchanged: the stored/returned enroll_url is exactly
        # what was passed in, trailing slash and all.
        assert result.enroll_url == f"{fake_server}/"

    def test_renew_strips_trailing_slash_before_appending_the_path(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        identity = _identity_for_renew(
            fake_server, f"{fake_renew_server}/", tmp_path, "renew-ok"
        )
        assert identity.enroll_url == f"{fake_renew_server}/"  # renew_url absent -> falls back

        captured_urls: list[str] = []
        real_request = identity_mod.urllib.request.Request

        def spy_request(url: str, *args: object, **kwargs: object) -> object:
            captured_urls.append(url)
            return real_request(url, *args, **kwargs)

        monkeypatch.setattr(identity_mod.urllib.request, "Request", spy_request)

        result = identity_mod.renew(identity)

        assert result is not None
        assert captured_urls == [f"{fake_renew_server}/v1/renew"]  # no double slash


def test_identity_module_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.identity", raising=False)
    importlib.import_module("src.sink.publish.identity")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )


def _iso_in(now_epoch: float, days: float) -> str:
    """An ISO-8601 "Z" timestamp `days` (may be negative/fractional) from `now_epoch`."""
    dt = datetime.datetime.fromtimestamp(now_epoch, tz=datetime.UTC) + datetime.timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class TestShouldAttemptRenewal:
    def test_29_days_out_no_prior_attempt_is_due(self) -> None:
        now = time.time()
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 29), None) is True

    def test_30_days_out_boundary_is_due(self) -> None:
        now = time.time()
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 30), None) is True

    def test_31_days_out_is_not_due(self) -> None:
        now = time.time()
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 31), None) is False

    def test_already_past_expiry_is_due(self) -> None:
        now = time.time()
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, -5), None) is True

    def test_23h_since_last_attempt_is_not_due(self) -> None:
        now = time.time()
        last_attempt = now - 23 * 3600
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 5), last_attempt) is False

    def test_25h_since_last_attempt_is_due(self) -> None:
        now = time.time()
        last_attempt = now - 25 * 3600
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 5), last_attempt) is True

    def test_exactly_86400s_since_last_attempt_boundary_is_due(self) -> None:
        now = time.time()
        last_attempt = now - 86400
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 5), last_attempt) is True

    def test_no_prior_attempt_within_window_is_due(self) -> None:
        now = time.time()
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 1), None) is True

    def test_outside_window_ignores_last_attempt_recency(self) -> None:
        # Even a last attempt from a second ago doesn't make this due --
        # the expiry window gates first, independent of rate limiting.
        now = time.time()
        last_attempt = now - 1
        assert identity_mod.should_attempt_renewal(now, _iso_in(now, 60), last_attempt) is False

    def test_accepts_a_non_z_utc_offset(self) -> None:
        now = time.time()
        expires_dt = datetime.datetime.fromtimestamp(
            now, tz=datetime.timezone(datetime.timedelta(hours=5))
        ) + datetime.timedelta(days=1)
        assert identity_mod.should_attempt_renewal(now, expires_dt.isoformat(), None) is True

    def test_naive_timestamp_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="timezone"):
            identity_mod.should_attempt_renewal(time.time(), "2027-01-01T00:00:00", None)


class TestRenewHappyPath:
    def test_writes_new_files_under_fresh_basenames_and_repoints_identity_yaml(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        old_key_bytes = identity.key_path.read_bytes()
        old_cert_bytes = identity.cert_path.read_bytes()
        old_key_path, old_cert_path = identity.key_path, identity.cert_path

        result = identity_mod.renew(identity)

        assert result is not None
        assert result.expires_at == "2028-01-01T00:00:00Z"
        assert result.tenant == identity.tenant
        assert result.broker_url == identity.broker_url

        # The pointer scheme: renew() never overwrites the old paths in
        # place -- the new files live under fresh basenames.
        assert result.key_path != old_key_path
        assert result.cert_path != old_cert_path
        assert result.key_path.parent == old_key_path.parent

        # New key/cert bytes differ from the old ones -- a fresh key was
        # generated, not the old one re-used (spec §6, read strictly).
        assert result.key_path.read_bytes() != old_key_bytes
        assert result.cert_path.read_bytes() != old_cert_bytes

        mode = stat.S_IMODE(result.key_path.stat().st_mode)
        assert mode == 0o600

        # The old files are unlinked (best-effort cleanup) once the pointer
        # swap has committed.
        assert not old_key_path.exists()
        assert not old_cert_path.exists()

        doc = yaml.safe_load((result.key_path.parent / "identity.yaml").read_text())
        assert doc["expires_at"] == "2028-01-01T00:00:00Z"
        assert doc["key_file"] == result.key_path.name
        assert doc["cert_file"] == result.cert_path.name
        assert doc["ca_file"] == result.ca_path.name
        assert "last_renewal_attempt_at" in doc

    def test_ca_replaced_under_a_fresh_basename_when_response_includes_ca_pem(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        old_ca_bytes = identity.ca_path.read_bytes()
        old_ca_path = identity.ca_path

        result = identity_mod.renew(identity)

        assert result is not None
        assert result.ca_path != old_ca_path
        assert result.ca_path.read_bytes() != old_ca_bytes
        assert not old_ca_path.exists()  # unlinked once the swap committed

    def test_ca_pointer_unchanged_when_response_omits_ca_pem(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The default "renew-ok" scenario's response always carries ca_pem;
        # this drives the "server didn't re-issue the CA" branch by
        # stripping it out of the parsed response before renew() inspects it.
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        old_ca_path = identity.ca_path
        old_ca_bytes = old_ca_path.read_bytes()

        real_loads = json.loads

        def strip_ca_pem(raw: object) -> object:
            payload = real_loads(raw)
            if isinstance(payload, dict):
                payload.pop("ca_pem", None)
            return payload

        monkeypatch.setattr(identity_mod.json, "loads", strip_ca_pem)

        result = identity_mod.renew(identity)

        assert result is not None
        assert result.ca_path == old_ca_path  # pointer untouched
        assert result.ca_path.read_bytes() == old_ca_bytes  # never rewritten
        assert old_ca_path.exists()  # never unlinked either

    def test_new_key_is_a_valid_matching_pair_with_new_cert(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        from cryptography import x509
        from cryptography.hazmat.primitives import serialization

        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        result = identity_mod.renew(identity)
        assert result is not None

        private_key = serialization.load_pem_private_key(
            result.key_path.read_bytes(), password=None
        )
        cert = x509.load_pem_x509_certificate(result.cert_path.read_bytes())
        assert cert.public_key().public_numbers() == private_key.public_key().public_numbers()

    def test_returned_identity_round_trips_through_load_identity(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        result = identity_mod.renew(identity)
        assert result is not None
        loaded = identity_mod.load_identity(identity.key_path.parent)
        assert loaded.expires_at == result.expires_at


class TestRenewPointerCommitCrashSemantics:
    """CRITICAL fix: identity.yaml is the single atomic commit point.

    A crash anywhere before the identity.yaml swap must leave the pointer
    resolving to the old, complete, matched, USABLE pair (new files are
    orphans); a crash at/after the swap means the pointer already names a
    complete new pair (all its files were fully written before the swap).
    There is no reachable state where the pointer names a mismatched pair.
    """

    def test_crash_before_the_yaml_swap_leaves_the_old_pair_pointed_at_and_usable(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        directory = identity.key_path.parent
        old_key_path, old_cert_path, old_ca_path = (
            identity.key_path,
            identity.cert_path,
            identity.ca_path,
        )
        files_before = {p.name for p in directory.iterdir()}

        real_write_identity_yaml = identity_mod._write_identity_yaml

        def crash_on_pointer_swap(
            renewed_identity: "identity_mod.Identity", *, key_file: str, **kwargs: object
        ) -> None:
            if key_file != renewed_identity.key_path.name:
                # This IS the success-path pointer swap (a fresh, "robot-"
                # versioned basename, never the identity's current one) --
                # simulate a crash exactly here, before os.replace commits it.
                raise RuntimeError("simulated crash during the identity.yaml swap")
            # Otherwise it's the failure-path _record_renewal_attempt call
            # (identity's own CURRENT basenames) -- let that one through so
            # the attempt still gets recorded, same as a real process would
            # manage on its next tick.
            return real_write_identity_yaml(renewed_identity, key_file=key_file, **kwargs)

        monkeypatch.setattr(identity_mod, "_write_identity_yaml", crash_on_pointer_swap)

        result = identity_mod.renew(identity)

        assert result is None  # renew() caught the simulated crash; never raised

        # New versioned files WERE written before the simulated crash (step
        # 1 of the pointer-commit scheme) -- they exist on disk as orphans,
        # but nothing points at them yet.
        files_after = {p.name for p in directory.iterdir()}
        new_orphans = files_after - files_before
        assert new_orphans  # step 1's writes landed

        # The old pointer is untouched: load_identity still resolves to the
        # OLD, complete pair.
        loaded = identity_mod.load_identity(directory)
        assert loaded.key_path == old_key_path
        assert loaded.cert_path == old_cert_path
        assert loaded.ca_path == old_ca_path
        assert old_key_path.exists()
        assert old_cert_path.exists()
        assert old_ca_path.exists()

        # And that old pair is still genuinely USABLE -- building the mTLS
        # context over it (load_cert_chain/load_verify_locations) succeeds,
        # proving it's a matched pair, not the mismatched hazard this scheme
        # exists to make unreachable.
        identity_mod._build_mtls_context(loaded)  # must not raise

        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        assert "last_renewal_attempt_at" in doc  # the fallback attempt-record got through
        assert doc["key_file"] == old_key_path.name  # pointer never moved

    def test_crash_after_the_yaml_swap_the_new_pair_is_complete_and_usable(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        # There's no code between the yaml swap and the return that can
        # meaningfully "crash" (best-effort unlinks swallow their own
        # errors) -- so this documents the postcondition directly: once the
        # swap has happened at all, everything it points at was already
        # fully written by step 1, so the new pair is immediately complete
        # and independently usable, exactly like the old pair was pre-swap.
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        directory = identity.key_path.parent

        result = identity_mod.renew(identity)

        assert result is not None
        loaded = identity_mod.load_identity(directory)
        assert loaded.key_path == result.key_path
        assert loaded.cert_path == result.cert_path
        assert loaded.ca_path == result.ca_path
        identity_mod._build_mtls_context(loaded)  # must not raise: a matched pair

        doc = yaml.safe_load((directory / "identity.yaml").read_text())
        assert doc["key_file"] == result.key_path.name
        assert doc["cert_file"] == result.cert_path.name
        assert doc["ca_file"] == result.ca_path.name

    def test_unlink_failure_during_cleanup_does_not_fail_the_renewal(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Step 3 (best-effort cleanup) is disk hygiene, not correctness: the
        # swap already committed in step 2, so a failure removing the
        # now-orphaned old files must not turn a successful renewal into a
        # failed one.
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        old_key_path = identity.key_path

        real_unlink = pathlib.Path.unlink

        def flaky_unlink(self: pathlib.Path, *args: object, **kwargs: object) -> None:
            if self == old_key_path:
                raise OSError("simulated permission denied")
            return real_unlink(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "unlink", flaky_unlink)

        result = identity_mod.renew(identity)

        assert result is not None  # the swap succeeded; cleanup failure is swallowed
        assert result.key_path.exists()
        assert old_key_path.exists()  # the failed-to-unlink orphan is simply left behind


class TestRenewMtlsContext:
    def test_load_cert_chain_and_verify_locations_called_with_identity_paths(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Spy on the real ssl.SSLContext methods (still calling through to
        # the real implementation) rather than faking the transport -- the
        # POST itself still goes out for real, just over plain HTTP to the
        # fake server (urlopen ignores context= for an http:// URL); this
        # only asserts the mTLS context was BUILT with the right material.
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        cert_chain_calls: list[tuple] = []
        verify_calls: list[tuple] = []
        original_load_cert_chain = ssl.SSLContext.load_cert_chain
        original_load_verify_locations = ssl.SSLContext.load_verify_locations

        def spy_load_cert_chain(self: ssl.SSLContext, *args: object, **kwargs: object) -> None:
            cert_chain_calls.append((args, kwargs))
            return original_load_cert_chain(self, *args, **kwargs)

        def spy_load_verify_locations(
            self: ssl.SSLContext, *args: object, **kwargs: object
        ) -> None:
            verify_calls.append((args, kwargs))
            return original_load_verify_locations(self, *args, **kwargs)

        monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", spy_load_cert_chain)
        monkeypatch.setattr(ssl.SSLContext, "load_verify_locations", spy_load_verify_locations)

        result = identity_mod.renew(identity)

        assert result is not None  # the real POST against the fake server still succeeded
        assert len(cert_chain_calls) == 1
        _, cert_chain_kwargs = cert_chain_calls[0]
        assert cert_chain_kwargs["certfile"] == str(identity.cert_path)
        assert cert_chain_kwargs["keyfile"] == str(identity.key_path)
        assert len(verify_calls) == 1
        _, verify_kwargs = verify_calls[0]
        assert verify_kwargs["cafile"] == str(identity.ca_path)


class TestRenewUnavailable:
    def test_501_returns_none_records_attempt_and_logs_info(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-501")
        old_expires_at = identity.expires_at

        with caplog.at_level(logging.INFO):
            result = identity_mod.renew(identity)

        assert result is None
        doc = yaml.safe_load((identity.key_path.parent / "identity.yaml").read_text())
        assert doc["expires_at"] == old_expires_at  # unchanged -- not a success
        assert "last_renewal_attempt_at" in doc  # attempt still recorded
        infos = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any("renewal" in r.getMessage().lower() for r in infos)
        warnings_ = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings_ == []  # 501 is expected/quiet, not a warning

    def test_404_returns_none_records_attempt_and_logs_info(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-404")

        with caplog.at_level(logging.INFO):
            result = identity_mod.renew(identity)

        assert result is None
        doc = yaml.safe_load((identity.key_path.parent / "identity.yaml").read_text())
        assert "last_renewal_attempt_at" in doc
        infos = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any("renewal" in r.getMessage().lower() for r in infos)


class TestRenewOtherFailures:
    def test_server_error_returns_none_records_attempt_and_logs_warning(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-500")

        with caplog.at_level(logging.WARNING):
            result = identity_mod.renew(identity)

        assert result is None
        doc = yaml.safe_load((identity.key_path.parent / "identity.yaml").read_text())
        assert "last_renewal_attempt_at" in doc
        warnings_ = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings_

    def test_malformed_200_response_returns_none(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-malformed")
        assert identity_mod.renew(identity) is None

    def test_non_json_200_response_returns_none(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-not-json")
        assert identity_mod.renew(identity) is None

    def test_connection_refused_returns_none_and_records_attempt(
        self, fake_server: str, tmp_path: object
    ) -> None:
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        identity = _identity_for_renew(
            fake_server, f"http://127.0.0.1:{port}", tmp_path, "renew-ok"
        )
        result = identity_mod.renew(identity)

        assert result is None
        doc = yaml.safe_load((identity.key_path.parent / "identity.yaml").read_text())
        assert "last_renewal_attempt_at" in doc


class TestRenewNeverRaises:
    def test_unexpected_exception_is_caught_logged_and_returns_none(
        self,
        fake_server: str,
        fake_renew_server: str,
        tmp_path: object,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")

        def boom(*_a: object, **_kw: object) -> tuple:
            raise RuntimeError("keygen exploded")

        monkeypatch.setattr(identity_mod, "generate_key_and_csr", boom)

        with caplog.at_level(logging.WARNING):
            result = identity_mod.renew(identity)  # must not raise

        assert result is None
        doc = yaml.safe_load((identity.key_path.parent / "identity.yaml").read_text())
        assert "last_renewal_attempt_at" in doc  # even an unexpected failure records the attempt
        warnings_ = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings_


class TestFailedRenewalLeavesIdentityIntact:
    """Self-review requirement: a failed renewal deletes nothing and changes no old bytes."""

    @pytest.mark.parametrize(
        "scenario", ["renew-501", "renew-404", "renew-500", "renew-malformed", "renew-not-json"]
    )
    def test_all_files_present_and_byte_identical_except_the_attempt_timestamp(
        self, fake_server: str, fake_renew_server: str, tmp_path: object, scenario: str
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, scenario)
        directory = identity.key_path.parent
        before = {p.name: p.read_bytes() for p in directory.iterdir()}

        result = identity_mod.renew(identity)

        assert result is None
        after_names = {p.name for p in directory.iterdir()}
        assert after_names == set(before)  # nothing deleted, nothing extra created
        for name, data in before.items():
            if name == "identity.yaml":
                continue  # last_renewal_attempt_at legitimately changes on every attempt
            assert directory.joinpath(name).read_bytes() == data, (
                f"{name} changed on a failed renewal -- old identity must stay intact"
            )
