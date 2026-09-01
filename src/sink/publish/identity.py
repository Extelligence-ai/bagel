"""Fleet enrollment and renewal client: keygen, CSR, identity storage (spec §6).

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

``renew()`` (see below) refreshes an existing identity's certificate ahead
of expiry: ``should_attempt_renewal`` decides when it is due, and ``renew``
does the mTLS POST + atomic file replacement. See ``renew()``'s docstring
for the ordering guarantees and what it does NOT do (force a live publisher
reconnect).
"""

import dataclasses
import datetime
import json
import logging
import os
import pathlib
import ssl
import tempfile
import time
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

_RENEWAL_WINDOW_S = 30 * 86400
_RENEWAL_MIN_INTERVAL_S = 86400
# Statuses the cloud uses to mean "renewal isn't offered on this deployment
# yet" (the flag is off) rather than "the request failed" -- expected, quiet.
_RENEWAL_NOT_OFFERED_STATUSES = (404, 501)


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

    Note: even when ``mode`` is omitted (``robot.crt``, ``ca.crt``,
    ``identity.yaml``), ``tempfile.mkstemp`` itself creates the temp file at 0600
    on POSIX, and ``os.replace`` preserves the source inode's mode -- so those
    files end up 0600 too, stricter than the spec requires (only ``robot.key``
    is mandated at 0600). Intentional, not a mode explicitly chosen for them;
    noted here for a future consumer who needs a world/group-readable cert.
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
    and never appears in a raised error's message: an other-status HTTPError
    body is echoed as a snippet for diagnostics, but the literal token is
    redacted from it first (defense against a debug-mode server, a reflecting
    proxy/WAF, or a malicious enroll endpoint handing the token straight back).

    Raises:
        ValueError: if ``enroll_url`` is not http(s).
        EnrollmentError: on a non-200 response (401 "unknown token", 410
            "used/expired", other statuses carry a token-redacted body
            snippet), a malformed 200 body (missing required fields), or a
            transport failure (status 0, the URLError reason).

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
        # The request body carried the token, so an echoing/reflecting server
        # (debug mode, a WAF, a malicious endpoint) could hand it straight back
        # in its error body. Redact before truncating -- never truncate first,
        # or a token straddling the cut point survives partially intact.
        body_text = exc.read().decode("utf-8", errors="replace")
        snippet = body_text.replace(token, "***REDACTED***")[:200]
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


def should_attempt_renewal(now: float, expires_at: str, last_attempt_at: float | None) -> bool:
    """Whether a renewal attempt is due: within 30 days of expiry, rate-limited to once/day.

    ``expires_at`` is an ISO-8601 timestamp with an explicit timezone (a bare
    "Z" suffix is accepted and treated as UTC -- that's the form the enroll
    and renew server responses use). ``now`` and ``last_attempt_at`` are Unix
    epoch seconds, matching ``time.time()`` -- the same clock ``renew()`` and
    ``HeartbeatThread`` use elsewhere.

    True iff the certificate expires within 30 days
    (``expires_epoch - now <= 30 * 86400``, inclusive: exactly 30 days out
    already counts as within the window) AND either no renewal has ever been
    attempted (``last_attempt_at is None``) or at least 86400s (1 day) have
    elapsed since the last one (inclusive: exactly 86400s counts as due
    again).

    Raises:
        ValueError: ``expires_at`` parses but carries no timezone.

    """
    expires_dt = datetime.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    if expires_dt.tzinfo is None:
        raise ValueError(f"expires_at must be timezone-aware: {expires_at!r}")
    within_window = (expires_dt.timestamp() - now) <= _RENEWAL_WINDOW_S
    if not within_window:
        return False
    if last_attempt_at is None:
        return True
    return (now - last_attempt_at) >= _RENEWAL_MIN_INTERVAL_S


def _build_mtls_context(identity: Identity) -> ssl.SSLContext:
    """Build the mTLS client context for a renewal POST.

    The CURRENT cert/key pair authenticates this robot to the server
    (``load_cert_chain``); ``ca_path`` verifies the server's cert
    (``load_verify_locations``). Broken out as its own function so unit
    tests can monkeypatch/spy the construction (e.g. wrap
    ``ssl.SSLContext.load_cert_chain`` and assert it was called with
    ``identity.cert_path``/``identity.key_path``) without standing up a real
    TLS listener -- the real-TLS path is e2e territory. The actual POST in
    those tests hits a plain-HTTP in-test fake server: ``urlopen`` simply
    ignores ``context=`` for an ``http://`` URL, so the same code runs
    against both.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_cert_chain(certfile=str(identity.cert_path), keyfile=str(identity.key_path))
    context.load_verify_locations(cafile=str(identity.ca_path))
    return context


def _write_identity_yaml(
    identity: Identity, *, expires_at: str, last_renewal_attempt_at: float
) -> None:
    """Atomically (re)write ``identity.yaml`` alongside ``identity``'s key file.

    Used by both the success and failure paths of ``renew()``: on failure
    only ``last_renewal_attempt_at`` changes (``expires_at`` is echoed back
    unchanged); on success both are the new values. Like ``enroll()``, this
    rewrites the whole document from ``identity``'s known fields rather than
    reading-modifying-writing the file on disk, so any unknown key a future
    version added to it (other than the five ``load_identity`` requires plus
    this one) is not preserved across a renewal -- same tradeoff `enroll()`
    already makes.
    """
    directory = identity.key_path.parent
    doc = {
        "tenant": identity.tenant,
        "robot_id": identity.robot_id,
        "broker_url": identity.broker_url,
        "enroll_url": identity.enroll_url,
        "expires_at": expires_at,
        "last_renewal_attempt_at": last_renewal_attempt_at,
    }
    _atomic_write(directory / "identity.yaml", yaml.safe_dump(doc).encode())


def _record_renewal_attempt(identity: Identity, attempt_at: float) -> None:
    """Persist ``last_renewal_attempt_at`` with ``expires_at`` unchanged (a failed attempt)."""
    _write_identity_yaml(
        identity, expires_at=identity.expires_at, last_renewal_attempt_at=attempt_at
    )


def _renew_inner(  # noqa: PLR0911 -- one early return per distinct failure branch, each logged/recorded differently
    identity: Identity, now: float, logger: logging.Logger
) -> Identity | None:
    """Attempt the actual renewal; ``renew()`` wraps this in a catch-all safety net."""
    if not identity.enroll_url.startswith(("https://", "http://")):
        logger.warning("renewal skipped: enroll_url is not http(s): %s", identity.enroll_url)
        _record_renewal_attempt(identity, now)
        return None

    new_key_pem, new_csr_pem = generate_key_and_csr(common_name=identity.robot_id)

    body = json.dumps({"csr_pem": new_csr_pem.decode()}).encode()
    request = urllib.request.Request(  # noqa: S310 -- scheme enforced to http(s) above
        f"{identity.enroll_url}/v1/renew",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    context = _build_mtls_context(identity)
    try:
        # urlopen raises HTTPError on non-2xx; context= is a no-op for the
        # http:// scheme a test's fake server uses, exercised for real only
        # against an https:// enroll_url in production.
        with urllib.request.urlopen(request, context=context, timeout=30) as response:  # noqa: S310
            raw = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        # No token in a renewal request, so (unlike enroll()) nothing needs
        # redacting before this snippet goes to the log -- see the module's
        # renew() docstring.
        snippet = exc.read().decode("utf-8", errors="replace")[:200]
        if status in _RENEWAL_NOT_OFFERED_STATUSES:
            logger.info("renewal not offered by server yet (status %d): %s", status, snippet)
        else:
            logger.warning("renewal request failed (status %d): %s", status, snippet)
        _record_renewal_attempt(identity, now)
        return None
    except urllib.error.URLError as exc:
        logger.warning("renewal request failed: %s", exc.reason)
        _record_renewal_attempt(identity, now)
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("renewal response malformed: %s", exc)
        _record_renewal_attempt(identity, now)
        return None
    if not isinstance(payload, dict):
        logger.warning("renewal response malformed: not a JSON object")
        _record_renewal_attempt(identity, now)
        return None
    missing = [field for field in ("cert_pem", "expires_at") if field not in payload]
    if missing:
        logger.warning("renewal response missing fields: %s", missing)
        _record_renewal_attempt(identity, now)
        return None

    # Key first (0600), then the cert, then (if offered) the CA, then the
    # yaml pointer document last -- same crash-safety ordering as enroll():
    # a crash mid-sequence never leaves a readable key with a stale cert
    # alongside it plus an identity.yaml that still claims the old expiry.
    _atomic_write(identity.key_path, new_key_pem, mode=0o600)
    _atomic_write(identity.cert_path, payload["cert_pem"].encode())
    if "ca_pem" in payload:
        _atomic_write(identity.ca_path, payload["ca_pem"].encode())
    _write_identity_yaml(identity, expires_at=payload["expires_at"], last_renewal_attempt_at=now)

    return dataclasses.replace(identity, expires_at=payload["expires_at"])


def renew(identity: Identity) -> Identity | None:
    """Renew ``identity``'s certificate: a NEW key + CSR, POSTed over mTLS.

    Generates a brand new private key and CSR -- not a re-use of the current
    key -- and POSTs it to ``{identity.enroll_url}/v1/renew`` authenticated
    with the CURRENT cert/key pair over mTLS (``ssl.SSLContext`` +
    ``load_cert_chain``/``load_verify_locations``, see
    ``_build_mtls_context``). Per spec §6, renewal happens "with a new CSR";
    read strictly, a new CSR implies a new key backing it, so a compromised
    old key is never perpetuated across a renewal.

    On a 200 response, ``robot.key`` (0600), ``robot.crt``, ``ca.crt`` (only
    if the response carries a ``ca_pem`` -- the server need not re-issue the
    CA on every renewal), and ``identity.yaml`` (updated ``expires_at`` and
    ``last_renewal_attempt_at``) are all replaced atomically, and the
    returned ``Identity`` reflects the new files. On any failure below --
    including a 501/404 meaning the server does not offer renewal on this
    deployment yet -- NOTHING is written except ``last_renewal_attempt_at``
    in ``identity.yaml``: the old key/cert/ca and ``expires_at`` are left
    completely untouched, so a failed renewal never leaves the robot with a
    broken or missing identity.

    IMPORTANT: this only replaces files on disk (and the in-memory
    ``Identity`` this function returns). The LIVE MQTT publisher connection
    keeps authenticating with the OLD cert until its own next reconnect --
    renewing does not force a reconnect. A caller that wants the running
    connection to pick up the new cert must swap its ``Identity`` reference
    to the returned value and either wait for the publisher's normal
    reconnect cycle or trigger one itself.

    Returns:
        The new ``Identity`` on a 200 response with a well-formed body.
        ``None`` if the server does not offer renewal yet (501/404 -- logged
        at INFO: the cloud ships renewal disabled behind a flag, so this is
        the expected path for now) or on any other failure (transport
        error, non-200/non-(501/404) status, malformed body, or an
        unexpected exception -- logged at WARNING). Either way, an attempt
        is recorded to ``identity.yaml`` before returning so
        ``should_attempt_renewal`` backs off correctly on the next tick.

        Never raises: every exception this function can encounter is caught
        here, logged, and turned into a ``None`` return with the attempt
        recorded -- a renewal hiccup must never be able to take down the
        heartbeat thread that drives it (see ``HeartbeatThread``'s
        ``renewal_check`` hook).

    """
    logger = logging.getLogger(__name__)
    now = time.time()
    try:
        return _renew_inner(identity, now, logger)
    except Exception as exc:  # renew() must never raise -- see docstring
        logger.warning("renewal failed unexpectedly: %s", exc, exc_info=True)
        try:
            _record_renewal_attempt(identity, now)
        except Exception:  # best-effort; must not mask the original failure
            logger.warning("failed to record renewal attempt after unexpected error", exc_info=True)
        return None
