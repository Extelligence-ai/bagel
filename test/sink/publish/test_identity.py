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
import ssl
import stat
import sys
import threading
import time

import pytest
import yaml

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
        body = json.dumps(
            {
                "cert_pem": robot_cert_pem.decode(),
                "ca_pem": ca_cert_pem.decode(),
                "broker_url": "mqtts://fleet.example.com:8883",
                "tenant": "acme",
                "robot_id": "robot-42",
                "expires_at": "2027-01-01T00:00:00Z",
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


class TestIsEnrolled:
    def test_true_after_enroll(self, fake_server: str, tmp_path: object) -> None:
        directory = tmp_path / "identity"
        identity_mod.enroll(VALID_TOKEN, fake_server, directory)
        assert identity_mod.is_enrolled(directory) is True

    def test_false_on_empty_dir(self, tmp_path: object) -> None:
        assert identity_mod.is_enrolled(tmp_path / "nope") is False


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
    def test_replaces_key_and_cert_and_updates_identity_yaml(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        old_key_bytes = identity.key_path.read_bytes()
        old_cert_bytes = identity.cert_path.read_bytes()

        result = identity_mod.renew(identity)

        assert result is not None
        assert result.expires_at == "2028-01-01T00:00:00Z"
        assert result.key_path == identity.key_path
        assert result.cert_path == identity.cert_path
        assert result.ca_path == identity.ca_path
        assert result.tenant == identity.tenant
        assert result.broker_url == identity.broker_url

        # New key bytes on disk differ from the old ones -- a fresh key was
        # generated, not the old one re-used (spec §6, read strictly).
        assert identity.key_path.read_bytes() != old_key_bytes
        assert identity.cert_path.read_bytes() != old_cert_bytes

        mode = stat.S_IMODE(identity.key_path.stat().st_mode)
        assert mode == 0o600

        doc = yaml.safe_load((identity.key_path.parent / "identity.yaml").read_text())
        assert doc["expires_at"] == "2028-01-01T00:00:00Z"
        assert "last_renewal_attempt_at" in doc

    def test_ca_replaced_when_response_includes_ca_pem(
        self, fake_server: str, fake_renew_server: str, tmp_path: object
    ) -> None:
        identity = _identity_for_renew(fake_server, fake_renew_server, tmp_path, "renew-ok")
        old_ca_bytes = identity.ca_path.read_bytes()

        identity_mod.renew(identity)

        assert identity.ca_path.read_bytes() != old_ca_bytes

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
