"""Fleet enrollment client: keygen, CSR, identity storage (spec §6).

The private key never leaves the process: it is generated on the robot,
only the CSR (public key + advisory CN) crosses the network. ``cryptography``
is imported lazily (inside functions) so importing this module never trips
the package's no-eager-import invariant alongside paho.

Identity is stored as four files under a directory (default
``~/.bagel/identity``, see ``settings.FLEET_IDENTITY_DIRECTORY``):
``robot.key`` (mode 0600), ``robot.crt``, ``ca.crt``, and ``identity.yaml``
(tenant, robot_id, broker_url, enroll_url, expires_at). All writes are
atomic (sibling tempfile + os.replace).

The enrollment token appears in the POST body only. It is never logged,
never stored, and never embedded in an ``EnrollmentError`` reason.
"""

import dataclasses
import json
import os
import pathlib
import tempfile
import urllib.error
import urllib.request

import yaml

from src.sink.publish import EnrollmentError, FleetNotEnrolledError

_REQUIRED_RESPONSE_FIELDS = (
    "cert_pem",
    "ca_pem",
    "broker_url",
    "tenant",
    "robot_id",
    "expires_at",
)


@dataclasses.dataclass
class Identity:
    """A robot's enrolled fleet identity: certs on disk plus broker metadata."""

    tenant: str
    robot_id: str
    broker_url: str
    enroll_url: str
    expires_at: str
    key_path: pathlib.Path
    cert_path: pathlib.Path
    ca_path: pathlib.Path

    @property
    def robot(self) -> str:
        """The tenant/robot_id identity string used by Spool.for_robot / MqttPublisher."""
        return f"{self.tenant}/{self.robot_id}"


def _atomic_write(target: pathlib.Path, data: bytes, *, mode: int | None = None) -> None:
    """Write ``data`` to ``target`` atomically via a sibling tempfile + os.replace.

    When ``mode`` is given, the temp file's fd is chmod'd before the write so the
    file lands at that mode the instant it becomes visible at ``target`` -- there
    is no window where the final path exists with looser permissions.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    tmp = pathlib.Path(tmp_name)
    try:
        if mode is not None:
            os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)


def generate_key_and_csr(common_name: str = "robot") -> tuple[bytes, bytes]:
    """Generate an EC (SECP256R1) private key and a CSR for it.

    Returns:
        (private_key_pem, csr_pem): the private key as PKCS8 PEM (unencrypted --
        it never leaves the process) and the CSR as PEM. The CSR's subject CN is
        advisory only: the enrollment server assigns the real robot identity and
        forces the CN it signs against.

    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    private_key = ec.generate_private_key(ec.SECP256R1())
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    csr = x509.CertificateSigningRequestBuilder(
        subject_name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    ).sign(private_key, hashes.SHA256())
    csr_pem = csr.public_bytes(serialization.Encoding.PEM)
    return private_key_pem, csr_pem


def enroll(token: str, enroll_url: str, directory: pathlib.Path) -> Identity:
    """Enroll this robot: keygen, POST the CSR, store the returned identity.

    POSTs ``{"token": token, "csr_pem": ...}`` to ``{enroll_url}/v1/enroll``. On a
    200 response, writes ``robot.key`` (0600), ``robot.crt``, ``ca.crt``, and
    ``identity.yaml`` atomically under ``directory`` and returns the resulting
    Identity. The token is sent in the request body only -- it is never logged
    and never appears in a raised error's message.

    Raises:
        ValueError: if ``enroll_url`` is not http(s).
        EnrollmentError: on a non-200 response (401 "unknown token", 410
            "used/expired", other statuses carry a body snippet), a malformed
            200 body (missing required fields), or a transport failure
            (status 0, the URLError reason).

    """
    if not enroll_url.startswith(("https://", "http://")):
        raise ValueError(f"enroll_url must be http(s): {enroll_url}")

    private_key_pem, csr_pem = generate_key_and_csr()

    body = json.dumps({"token": token, "csr_pem": csr_pem.decode()}).encode()
    request = urllib.request.Request(  # noqa: S310 -- scheme enforced to http(s) above
        f"{enroll_url}/v1/enroll",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        # urlopen raises HTTPError on non-2xx, so a rejected/failed enroll
        # takes the except branches below rather than falling through.
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        snippet = exc.read().decode("utf-8", errors="replace")[:200]
        if status == 401:  # noqa: PLR2004 -- protocol status code, not a magic threshold
            reason = "unknown token"
        elif status == 410:  # noqa: PLR2004 -- protocol status code, not a magic threshold
            reason = "used/expired"
        else:
            reason = snippet or (exc.reason if isinstance(exc.reason, str) else str(exc.reason))
        raise EnrollmentError(status, reason) from exc
    except urllib.error.URLError as exc:
        raise EnrollmentError(0, str(exc.reason)) from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise EnrollmentError(status, f"malformed response: {exc}") from exc

    if not isinstance(payload, dict):
        raise EnrollmentError(status, "malformed response: not a JSON object")

    missing = [field for field in _REQUIRED_RESPONSE_FIELDS if field not in payload]
    if missing:
        raise EnrollmentError(status, f"malformed response: missing {missing}")

    directory = pathlib.Path(directory)
    key_path = directory / "robot.key"
    cert_path = directory / "robot.crt"
    ca_path = directory / "ca.crt"
    identity_path = directory / "identity.yaml"

    # Key first (0600), then the certs, then the yaml pointer document --
    # a crash mid-sequence never leaves a readable key with no cert alongside it.
    _atomic_write(key_path, private_key_pem, mode=0o600)
    _atomic_write(cert_path, payload["cert_pem"].encode())
    _atomic_write(ca_path, payload["ca_pem"].encode())

    doc = {
        "tenant": payload["tenant"],
        "robot_id": payload["robot_id"],
        "broker_url": payload["broker_url"],
        "enroll_url": enroll_url,
        "expires_at": payload["expires_at"],
    }
    _atomic_write(identity_path, yaml.safe_dump(doc).encode())

    return Identity(
        tenant=payload["tenant"],
        robot_id=payload["robot_id"],
        broker_url=payload["broker_url"],
        enroll_url=enroll_url,
        expires_at=payload["expires_at"],
        key_path=key_path,
        cert_path=cert_path,
        ca_path=ca_path,
    )


def load_identity(directory: pathlib.Path) -> Identity:
    """Load a previously stored Identity from ``directory``.

    Raises:
        FleetNotEnrolledError: if identity.yaml is missing, unparsable, missing a
            required field, or any of the three PEM files is absent.

    """
    directory = pathlib.Path(directory)
    identity_path = directory / "identity.yaml"
    key_path = directory / "robot.key"
    cert_path = directory / "robot.crt"
    ca_path = directory / "ca.crt"

    try:
        text = identity_path.read_text()
    except OSError as exc:
        raise FleetNotEnrolledError(f"no identity at {directory}: {exc}") from exc

    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise FleetNotEnrolledError(f"corrupt identity.yaml at {identity_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise FleetNotEnrolledError(f"corrupt identity.yaml at {identity_path}: not a mapping")

    # .get() rather than [] indexing: keeps this loader tolerant of unknown/future
    # optional keys (e.g. a later last_renewal_attempt_at) -- only these five are
    # required today.
    tenant = data.get("tenant")
    robot_id = data.get("robot_id")
    broker_url = data.get("broker_url")
    enroll_url = data.get("enroll_url")
    expires_at = data.get("expires_at")
    missing = [
        name
        for name, value in (
            ("tenant", tenant),
            ("robot_id", robot_id),
            ("broker_url", broker_url),
            ("enroll_url", enroll_url),
            ("expires_at", expires_at),
        )
        if value is None
    ]
    if missing:
        raise FleetNotEnrolledError(f"identity.yaml at {identity_path} missing: {missing}")

    for path in (key_path, cert_path, ca_path):
        if not path.is_file():
            raise FleetNotEnrolledError(f"missing identity file: {path}")

    return Identity(
        tenant=tenant,
        robot_id=robot_id,
        broker_url=broker_url,
        enroll_url=enroll_url,
        expires_at=expires_at,
        key_path=key_path,
        cert_path=cert_path,
        ca_path=ca_path,
    )


def is_enrolled(directory: pathlib.Path) -> bool:
    """Return True iff a complete, parsable identity is stored under ``directory``."""
    try:
        load_identity(directory)
    except FleetNotEnrolledError:
        return False
    return True
