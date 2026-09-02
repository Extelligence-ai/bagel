"""Fleet enrollment and renewal client: keygen, CSR, identity storage (spec §6).

The private key never leaves the process: it is generated on the robot,
only the CSR (public key + advisory CN) crosses the network. ``cryptography``
is imported lazily (inside functions) so importing this module never trips
the package's no-eager-import invariant alongside paho.

Identity is stored as four files under a directory (default
``~/.bagel/identity``, see ``settings.FLEET_IDENTITY_DIRECTORY``): a key
(mode 0600), a cert, a CA cert, and ``identity.yaml``, which carries
``tenant``, ``robot_id``, ``broker_url``, ``enroll_url``, ``expires_at``,
and -- a pointer scheme -- the OPTIONAL ``key_file``/``cert_file``/
``ca_file`` basenames naming the other three files. ``enroll()`` always
writes them (as ``robot.key``/``robot.crt``/``ca.crt``, the historical fixed
names) so every identity this module writes carries an explicit pointer;
``load_identity`` defaults each to its historical fixed name when absent, so
an identity.yaml written before this scheme existed still loads unchanged.
``renew()`` is the reason the pointer is optional/overridable in the first
place: each successful renewal writes its new key/cert (and CA, if the
server issued one) under fresh, never-before-used basenames rather than
overwriting the files identity.yaml currently points at -- see its
docstring for why. All writes are atomic (sibling tempfile + os.replace).

``enroll_url`` and the OPTIONAL ``renew_url`` are both BASE urls (no
trailing path) -- the client appends ``/v1/enroll`` and ``/v1/renew``
itself. A trailing slash on the configured base is tolerated: it is
stripped before the path is appended (so ``.../v1/enroll``, never
``..//v1/enroll``), but the BASE value itself -- as stored in
``identity.yaml`` and on the returned ``Identity`` -- is left exactly as
given. They commonly point at different hosts entirely: enroll is a
path-routed HTTPS endpoint (e.g. an ALB), while renew is an mTLS listener on
the broker itself, which is a separate host:port -- so ``renew()`` targets
``identity.renew_url`` when the enroll response carried one, falling back to
``identity.enroll_url`` (today's behavior) when it didn't.

The enrollment token appears in the POST body only. It is never logged,
never stored, and never embedded in an ``EnrollmentError`` reason.

``renew()`` (see below) refreshes an existing identity's certificate ahead
of expiry: ``should_attempt_renewal`` decides when it is due, and ``renew``
does the mTLS POST + atomic pointer swap. See ``renew()``'s docstring for
the ordering guarantees and what it does NOT do (force a live publisher
reconnect). The renew response may itself carry a (new) ``renew_url``,
rotating the base a future renewal targets -- e.g. a staging->prod cutover --
without requiring a fresh enrollment.

``delete_identity()`` is the ONLY deletion path ``unenroll`` uses: it removes
the pointer set from a parsable identity.yaml plus anything on disk matching
this module's own renewal-orphan naming patterns, all resolved-containment
checked against the target directory first -- see its own docstring.
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
from urllib.parse import urlparse

import yaml

from settings import settings
from src.sink.publish import EnrollmentError, FleetNotEnrolledError
from src.sink.publish import connect as _connect

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

# delete_identity()'s pattern-glob sweep: the historical fixed names plus
# every versioned basename _commit_renewed_files can ever produce
# (``robot-<version>.key``/``.crt``, ``ca-<version>.crt``) -- this is how a
# renewal orphan (a superseded pair identity.yaml no longer points at, left
# behind by e.g. a crash during best-effort cleanup) still gets swept even
# though identity.yaml's pointer set no longer names it. Non-recursive
# (``Path.glob``, not ``rglob``) -- this module never nests files under
# subdirectories of the identity directory.
_DELETE_GLOB_PATTERNS = (
    "robot.key",
    "robot.crt",
    "ca.crt",
    "robot-*.key",
    "robot-*.crt",
    "ca-*.crt",
)


def _url_passes_scheme_gate(url: str) -> bool:
    """Whether `url` may be used for an enroll/renew POST (Codex review).

    Mirrors connect.py's own mqtt(s):// transport policy exactly, reusing
    (not duplicating) its ``_is_local_or_private`` helper: ``https://`` is
    always fine; plaintext ``http://`` is fine only when the host is
    loopback/private, or when ``settings.FLEET_DEV_INSECURE`` is set (local
    development only) -- otherwise the one-time enrollment token (or, for
    renew, the mTLS-authenticated request) would go out in cleartext to an
    arbitrary nonlocal host. Any other scheme (including no scheme at all)
    fails the gate; callers already reject those separately with a more
    specific error before this is even consulted.
    """
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return True
    if parsed.scheme != "http":
        return False
    if settings.FLEET_DEV_INSECURE:
        return True
    return bool(parsed.hostname) and _connect._is_local_or_private(parsed.hostname)


def _validate_enroll_url(enroll_url: str) -> None:
    """Validate `enroll()`'s URL: http(s) scheme, then the scheme gate.

    Split out of `enroll()` to keep that function's cyclomatic complexity in
    check after the Codex-review scheme-gate check added a branch to it.
    """
    if not enroll_url.startswith(("https://", "http://")):
        raise ValueError(f"enroll_url must be http(s): {enroll_url}")
    if not _url_passes_scheme_gate(enroll_url):
        raise EnrollmentError(0, "insecure enroll_url")


@dataclasses.dataclass
class Identity:
    """A robot's enrolled fleet identity: certs on disk plus broker metadata.

    ``renew_url``, when set, is where ``renew()`` targets its POST instead of
    ``enroll_url`` -- see the module docstring for why enroll and renew can
    live on different hosts. Defaults to ``None`` (no renew endpoint known:
    ``renew()`` falls back to ``enroll_url``) so every existing construction
    of this dataclass -- and every identity.yaml written before this field
    existed -- keeps working unchanged.
    """

    tenant: str
    robot_id: str
    broker_url: str
    enroll_url: str
    expires_at: str
    key_path: pathlib.Path
    cert_path: pathlib.Path
    ca_path: pathlib.Path
    renew_url: str | None = None
    last_renewal_attempt_at: float | None = None
    """Unix epoch seconds of the most recent renewal attempt, on disk or None.

    Populated by ``load_identity`` from identity.yaml's ``last_renewal_attempt_at``
    (tolerating an absent or wrong-typed value as ``None``) so
    ``FleetService`` can seed its own in-memory rate-limit tracking from it
    at construction time -- without this, the daily rate limit
    (``_RENEWAL_MIN_INTERVAL_S``) resets on every process restart, letting a
    crashlooping robot inside the 30-day renewal window fire a fresh renewal
    POST roughly every restart. Defaults to ``None`` so every existing
    construction of this dataclass keeps working unchanged.
    """

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
    # mode=0o700: the identity directory holds a private key plus mTLS
    # material, so it should never be group/other-readable. Only applies at
    # creation time -- an already-existing directory keeps whatever mode it
    # already has (exist_ok=True), which is fine: this only needs to nail
    # down the mode the FIRST time this directory is ever created.
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
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
    Identity. If the response carries an OPTIONAL ``renew_url`` (see the
    module docstring: renew commonly lives on a different host than enroll),
    it is stored too and ``renew()`` targets it instead of ``enroll_url``;
    absent, ``renew()`` falls back to ``enroll_url``. The token is sent in
    the request body only -- it is never logged
    and never appears in a raised error's message: an other-status HTTPError
    body is echoed as a snippet for diagnostics, but the literal token is
    redacted from it first (defense against a debug-mode server, a reflecting
    proxy/WAF, or a malicious enroll endpoint handing the token straight back).

    Raises:
        ValueError: if ``enroll_url`` is not http(s).
        EnrollmentError: on a non-200 response (401 "unknown token", 410
            "used/expired", other statuses carry a token-redacted body
            snippet), a malformed 200 body (missing required fields), a
            transport failure (status 0, the URLError reason), or an
            ``enroll_url`` that fails the scheme gate (status 0, reason
            "insecure enroll_url" -- see ``_url_passes_scheme_gate``: a
            nonlocal ``http://`` URL without ``FLEET_DEV_INSECURE`` would
            send the one-time token in cleartext).

    """
    _validate_enroll_url(enroll_url)

    private_key_pem, csr_pem = generate_key_and_csr()

    body = json.dumps({"token": token, "csr_pem": csr_pem.decode()}).encode()
    request = urllib.request.Request(  # noqa: S310 -- scheme enforced to http(s) above
        f"{enroll_url.rstrip('/')}/v1/enroll",
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

    # renew_url is OPTIONAL in the response (see the module docstring): a
    # server that doesn't send one means "renew from the same base as
    # enroll" -- so the key is only written when the response actually
    # carried it, keeping "absent" and "explicitly the same as enroll_url"
    # distinguishable on disk.
    renew_url = payload.get("renew_url")

    doc = {
        "tenant": payload["tenant"],
        "robot_id": payload["robot_id"],
        "broker_url": payload["broker_url"],
        "enroll_url": enroll_url,
        "expires_at": payload["expires_at"],
        # Pointer fields, written explicitly even though they equal
        # load_identity's own defaults -- see the module docstring: every
        # identity this module writes carries an explicit pointer, so the
        # pointer scheme is the one mechanism rather than "implicit legacy
        # fixed names vs. explicit renewed names".
        "key_file": key_path.name,
        "cert_file": cert_path.name,
        "ca_file": ca_path.name,
    }
    if renew_url is not None:
        doc["renew_url"] = renew_url
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
        renew_url=renew_url,
    )


def _resolve_pointer_field(
    data: dict, field_name: str, default_name: str, identity_path: pathlib.Path
) -> str:
    """Resolve one of identity.yaml's optional key_file/cert_file/ca_file pointers.

    Absent entirely -> ``default_name`` (the historical fixed name -- see
    ``load_identity``'s docstring). Present but not a ``str`` (e.g. an
    explicit ``key_file: null``) -> ``FleetNotEnrolledError``: unlike the
    "absent" case, ``.get(field, default)``'s default would NOT have applied
    here (the key is present), so silently falling back would hide a corrupt
    document; raising instead is what makes a hand-edited ``key_file: null``
    behave like "not enrolled" rather than a raw ``TypeError`` from
    ``directory / None`` deeper in the caller.
    """
    if field_name not in data:
        return default_name
    value = data[field_name]
    if not isinstance(value, str):
        raise FleetNotEnrolledError(
            f"identity.yaml at {identity_path} has non-string {field_name}: {value!r}"
        )
    return value


def _parse_last_renewal_attempt_at(data: dict) -> float | None:
    """Parse identity.yaml's OPTIONAL ``last_renewal_attempt_at``, tolerantly.

    Unlike the pointer fields, an absent or wrong-typed value here is not
    treated as corrupt -- there's no crash-risk equivalent of the
    ``directory / None`` hazard the pointer fields have, so this simply
    means "no attempt recorded yet" (``None``) rather than raising.
    """
    value = data.get("last_renewal_attempt_at")
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return value


def load_identity(directory: pathlib.Path) -> Identity:
    """Load a previously stored Identity from ``directory``.

    The key/cert/CA basenames come from identity.yaml's optional
    ``key_file``/``cert_file``/``ca_file`` pointer fields (see the module
    docstring), defaulting to the historical fixed names
    (``robot.key``/``robot.crt``/``ca.crt``) when a field is absent -- so an
    identity.yaml from before the pointer scheme, or one hand-written
    without it, still loads correctly.

    Raises:
        FleetNotEnrolledError: if identity.yaml is missing, unparsable, missing a
            required field, or any of the three PEM files is absent.

    """
    directory = pathlib.Path(directory)
    identity_path = directory / "identity.yaml"

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

    # expires_at is required (checked above), but a hand-edited or corrupt
    # document can still carry the right KEY with the wrong type -- notably
    # an unquoted ISO timestamp, which YAML's implicit resolver parses as a
    # datetime object, not str. should_attempt_renewal expects a str it can
    # call .replace() on, so treat a non-str value the same as "not enrolled"
    # rather than let a TypeError surface later, deep in the renewal path.
    if not isinstance(expires_at, str):
        raise FleetNotEnrolledError(
            f"identity.yaml at {identity_path} has non-string expires_at: {expires_at!r}"
        )

    # The three pointer fields are OPTIONAL (see the module docstring and
    # `_resolve_pointer_field`'s docstring for the absent-vs-wrong-type
    # distinction that matters here).
    key_path = directory / _resolve_pointer_field(data, "key_file", "robot.key", identity_path)
    cert_path = directory / _resolve_pointer_field(data, "cert_file", "robot.crt", identity_path)
    ca_path = directory / _resolve_pointer_field(data, "ca_file", "ca.crt", identity_path)
    renew_url = data.get("renew_url")  # optional -- absent means "fall back to enroll_url"
    last_renewal_attempt_at = _parse_last_renewal_attempt_at(data)

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
        renew_url=renew_url,
        last_renewal_attempt_at=last_renewal_attempt_at,
    )


def is_enrolled(directory: pathlib.Path) -> bool:
    """Return True iff a complete, parsable identity is stored under ``directory``."""
    try:
        load_identity(directory)
    except FleetNotEnrolledError:
        return False
    return True


def delete_identity(directory: pathlib.Path) -> list[str]:
    """Delete a stored identity under ``directory`` -- the ONLY deletion path ``unenroll`` uses.

    ``directory`` is resolved first, and every path this function is about
    to remove is resolved-containment checked against it (``Spool.for_robot``'s
    belt-and-braces, applied here to reads-become-deletes) before anything is
    unlinked:

    1. If ``directory / "identity.yaml"`` doesn't exist, steps 2 and 4's
       pointer-set/whole-file work are skipped, but the pattern sweep
       (step 3) still runs unconditionally (Codex round 3, P2) -- it's
       contained and non-recursive, so it's always safe, and it's the only
       way to clean up an enroll that crashed between writing key material
       and writing ``identity.yaml`` itself: without this, those orphaned
       key/cert files would never be swept, since nothing else ever points
       at them. Idempotent either way: a directory with nothing left in it
       returns ``[]`` on the next call.
    2. ``load_identity(directory)`` is tried (only when ``identity.yaml``
       exists). If it parses, the deletion set starts as ``{key_path,
       cert_path, ca_path}``: each must resolve INSIDE ``directory`` or this
       raises ``ValueError`` immediately -- BEFORE any unlink -- so a
       hand-edited ``key_file: ../../victim`` pointer can never delete
       outside the directory. If it doesn't parse (``FleetNotEnrolledError``
       -- a corrupt identity.yaml), this step is skipped entirely: a corrupt
       identity must still be removable via the pattern sweep below, not
       permanently stuck.
    3. This module's own naming patterns (see ``_DELETE_GLOB_PATTERNS``) are
       globbed inside ``directory`` (non-recursive) to additionally sweep up
       renewal orphans identity.yaml no longer points at. The same
       containment check applies per-entry here too, but a failure here
       SKIPS just that one entry rather than raising -- a symlink planted at
       one of these names that resolves outside ``directory`` is left
       completely alone (neither the link nor its target is touched); it
       simply isn't included in what gets deleted. Unlike step 2's pointer
       fields (which come from a trusted, single load), this is an
       opportunistic best-effort sweep over whatever happens to match a
       filename pattern, so one bad entry must not block cleanup of
       everything else.
    4. The union of steps 2+3, plus ``identity.yaml`` itself (only when it
       exists), is unlinked (``missing_ok=True``); then ``directory.rmdir()``
       is attempted, best-effort (``OSError`` -- e.g. "not empty", because a
       skipped symlink or an unrelated file is still in there, or the
       directory never existed at all -- is swallowed).

    Never uses ``shutil.rmtree``: only paths derived from steps 2 and 3
    above are ever touched.

    Returns:
        Sorted basenames of every file actually targeted for deletion (the
        pointer set that passed containment, the pattern-set matches that
        passed containment, and ``identity.yaml`` when it exists) -- not
        necessarily the basenames actually removed, since ``missing_ok=True``
        tolerates a concurrent removal, but in the ordinary case these
        coincide.

    Raises:
        ValueError: a pointer field in a parsable identity.yaml resolves
            outside ``directory``. Nothing is deleted when this is raised.

    """
    directory = pathlib.Path(directory).resolve()
    identity_path = directory / "identity.yaml"
    identity_yaml_exists = identity_path.is_file()

    to_delete: dict[str, pathlib.Path] = {}
    if identity_yaml_exists:
        to_delete.update(_delete_identity_pointer_set(directory))
    to_delete.update(_delete_identity_pattern_set(directory))
    if identity_yaml_exists:
        to_delete["identity.yaml"] = identity_path

    for path in to_delete.values():
        path.unlink(missing_ok=True)
    try:
        directory.rmdir()
    except OSError:
        pass  # not empty (a skipped entry or unrelated file remains), or never existed -- fine

    return sorted(to_delete)


def _delete_identity_pointer_set(directory: pathlib.Path) -> dict[str, pathlib.Path]:
    """``delete_identity`` step 2: the parsable identity.yaml's key/cert/ca pointers.

    ``directory`` is already resolved by the caller. Raises ``ValueError``
    (deleting nothing -- this is called before any unlink) if a pointer
    resolves outside ``directory``; returns ``{}`` if identity.yaml doesn't
    parse (``FleetNotEnrolledError``) -- a corrupt identity still falls
    through to the pattern-set sweep.
    """
    try:
        identity = load_identity(directory)
    except FleetNotEnrolledError:
        return {}
    found: dict[str, pathlib.Path] = {}
    for path in (identity.key_path, identity.cert_path, identity.ca_path):
        if not path.resolve().is_relative_to(directory):
            raise ValueError(f"identity file escapes {directory}: {path}")
        found[path.name] = path
    return found


def _delete_identity_pattern_set(directory: pathlib.Path) -> dict[str, pathlib.Path]:
    """``delete_identity`` step 3: sweep this module's own naming patterns.

    ``directory`` is already resolved by the caller. Unlike the pointer set,
    an entry that fails the containment check (e.g. a symlink resolving
    outside ``directory``) is simply SKIPPED, not raised -- this is an
    opportunistic best-effort sweep over whatever matches a filename
    pattern, so one bad entry must not block cleanup of everything else.
    """
    found: dict[str, pathlib.Path] = {}
    for pattern in _DELETE_GLOB_PATTERNS:
        for candidate in directory.glob(pattern):
            if not candidate.resolve().is_relative_to(directory):
                continue  # e.g. a symlink pointing outside -- skip, don't follow it
            found[candidate.name] = candidate
    return found


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


def _write_identity_yaml(  # noqa: PLR0913 -- one field per identity.yaml key; splitting hides the pointer's atomicity
    identity: Identity,
    *,
    expires_at: str,
    last_renewal_attempt_at: float,
    key_file: str,
    cert_file: str,
    ca_file: str,
    renew_url: str | None,
) -> None:
    """Atomically (re)write ``identity.yaml`` -- THE commit point for a renewal.

    This is a single ``_atomic_write`` (sibling tempfile + ``os.replace``, so
    it is itself atomic: readers only ever see the fully-old or fully-new
    document, never a partial one). ``renew()`` relies on that: it always
    finishes writing a COMPLETE, self-consistent key/cert/ca file set under
    fresh basenames *before* calling this, so this call is the one moment a
    renewal actually takes effect -- see ``_renew_inner``'s success path and
    ``renew()``'s docstring for the full crash-safety argument.

    Used by both the success and failure paths of ``renew()``: on failure,
    ``key_file``/``cert_file``/``ca_file``/``expires_at``/``renew_url`` are
    ``identity``'s CURRENT (unchanged) pointer/expiry/renew base -- only
    ``last_renewal_attempt_at`` moves; on success ``key_file``/``cert_file``/
    ``ca_file``/``expires_at`` are the new pointer and expiry, and
    ``renew_url`` is the caller-computed value (see ``_commit_renewed_files``:
    the renew response may itself rotate ``renew_url`` -- absent means no
    change, present means the new base). Like ``enroll()``, this rewrites
    the whole document from known fields rather than reading-modifying-
    writing the file on disk, so any unknown key a future version added to
    it is not preserved across a renewal -- same tradeoff ``enroll()``
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
        "key_file": key_file,
        "cert_file": cert_file,
        "ca_file": ca_file,
    }
    if renew_url is not None:
        doc["renew_url"] = renew_url
    _atomic_write(directory / "identity.yaml", yaml.safe_dump(doc).encode())


def _record_renewal_attempt(identity: Identity, attempt_at: float) -> None:
    """Persist ``last_renewal_attempt_at``; pointer and ``expires_at`` unchanged (a failed attempt).

    Passes ``identity``'s CURRENT file basenames straight through -- a
    failed attempt never moves the pointer, so this can't be the thing that
    turns a matched pair into a mismatched one (see ``renew()``'s
    docstring).
    """
    _write_identity_yaml(
        identity,
        expires_at=identity.expires_at,
        last_renewal_attempt_at=attempt_at,
        key_file=identity.key_path.name,
        cert_file=identity.cert_path.name,
        ca_file=identity.ca_path.name,
        renew_url=identity.renew_url,
    )


def _commit_renewed_files(
    identity: Identity, new_key_pem: bytes, payload: dict, now: float
) -> Identity:
    """Commit a renewal via the pointer scheme: write, then swap, then clean up.

    NOT an in-place overwrite of ``identity.key_path``/``cert_path``/
    ``ca_path`` -- overwriting those in place is exactly what let a crash
    between the key and cert writes leave identity.yaml's fixed pointer
    resolving to a mismatched new-key/old-cert pair, permanently:
    ``is_enrolled()`` still blesses it, and every subsequent renewal's
    ``_build_mtls_context`` fails the same way forever, since it's the same
    broken pair being loaded and re-authenticated with each time.

    Instead:
      1. Write the COMPLETE new file set under fresh, never-before-used
         basenames. identity.yaml still names the OLD files, which this step
         never touches -- so a crash anywhere in this step leaves the old
         pointer resolving to the old, complete, matched, USABLE pair; these
         new files are just inert orphans until step 2 commits.
      2. ONE atomic identity.yaml write (``_write_identity_yaml``, itself a
         single tempfile+os.replace) repoints
         ``key_file``/``cert_file``/``ca_file`` at the new basenames. This is
         the only moment the renewal takes effect: a crash before it leaves
         the old pointer untouched; a crash during/after it means
         ``os.replace`` either didn't happen (old pointer survives) or fully
         happened (new pointer, already fully written by step 1) -- never a
         mix. There is no reachable state where the pointer names a
         mismatched key/cert pair.
      3. Best-effort cleanup of the now-superseded old files. Step 2 already
         committed, so this is pure disk hygiene, not correctness: if it's
         skipped (a crash) or fails (permissions), the old files are just
         harmless, unreferenced orphans -- nothing in identity.yaml points at
         them anymore.

    ``payload`` must already be validated to carry ``cert_pem``/``expires_at``
    -- this function assumes a well-formed 200 response.
    """
    directory = identity.key_path.parent
    # Nanosecond-precision version tag: guarantees the new basenames never
    # collide with identity's current ones, or a prior renewal's run
    # back-to-back -- a collision would mean this renewal's step-1 writes
    # land on the file identity.yaml CURRENTLY points at, before the step-2
    # swap, which would recreate exactly the in-place-overwrite hazard this
    # scheme exists to avoid.
    version = time.time_ns()
    new_key_file = f"robot-{version}.key"
    new_cert_file = f"robot-{version}.crt"
    write_new_ca = "ca_pem" in payload
    new_ca_file = f"ca-{version}.crt" if write_new_ca else identity.ca_path.name

    _atomic_write(directory / new_key_file, new_key_pem, mode=0o600)
    _atomic_write(directory / new_cert_file, payload["cert_pem"].encode())
    if write_new_ca:
        _atomic_write(directory / new_ca_file, payload["ca_pem"].encode())

    # renew_url is OPTIONAL in the renew response too, same as enroll's (see
    # the module docstring): absent means "no change" (`.get`'s default
    # keeps identity's current value, whatever that already was), present
    # means the server is rotating the renew base -- e.g. a staging->prod
    # cutover -- without requiring a fresh enrollment.
    new_renew_url = payload.get("renew_url", identity.renew_url)
    _write_identity_yaml(
        identity,
        expires_at=payload["expires_at"],
        last_renewal_attempt_at=now,
        key_file=new_key_file,
        cert_file=new_cert_file,
        ca_file=new_ca_file,
        renew_url=new_renew_url,
    )

    for old_path in (identity.key_path, identity.cert_path):
        try:
            old_path.unlink()
        except OSError:
            pass  # orphan is inert -- identity.yaml no longer points at it
    if write_new_ca:
        try:
            identity.ca_path.unlink()
        except OSError:
            pass

    return dataclasses.replace(
        identity,
        expires_at=payload["expires_at"],
        key_path=directory / new_key_file,
        cert_path=directory / new_cert_file,
        ca_path=directory / new_ca_file,
        renew_url=new_renew_url,
        last_renewal_attempt_at=now,
    )


def _cert_matches_key(cert_pem: str, key_pem: bytes) -> bool:
    """Report whether ``cert_pem``'s public key matches the private key in ``key_pem``.

    Guards a renewal commit against a response whose ``cert_pem`` was issued
    for a different CSR (server bug, stale/replayed response, or a
    mismatched-CSR mixup) -- committing such a pair would repoint
    identity.yaml at a cert nothing on disk holds the matching private key
    for. Malformed PEM on either side is treated as a mismatch (returns
    False) rather than raising, so callers get one uniform reject path.
    """
    from cryptography import x509
    from cryptography.exceptions import UnsupportedAlgorithm
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    try:
        cert = x509.load_pem_x509_certificate(cert_pem.encode())
        private_key = load_pem_private_key(key_pem, password=None)
        cert_public_numbers = cert.public_key().public_numbers()
        key_public_numbers = private_key.public_key().public_numbers()
    except (ValueError, TypeError, UnsupportedAlgorithm):
        return False
    return cert_public_numbers == key_public_numbers


def _renew_inner(  # noqa: PLR0911 -- one early return per distinct failure branch, each logged/recorded differently
    identity: Identity, now: float, logger: logging.Logger
) -> Identity | None:
    """Attempt the actual renewal; ``renew()`` wraps this in a catch-all safety net."""
    # renew_url, when the enroll response carried one, else fall back to
    # enroll_url (today's behavior) -- see the module docstring: enroll and
    # renew commonly live on different hosts (enroll: path-routed HTTPS;
    # renew: an mTLS listener on the broker itself), so deriving the renew
    # target from enroll_url alone would 404 against a real cloud deployment.
    target_base = identity.renew_url or identity.enroll_url
    if not target_base.startswith(("https://", "http://")):
        logger.warning("renewal skipped: renew target is not http(s): %s", target_base)
        _record_renewal_attempt(identity, now)
        return None
    # Same scheme gate as enroll() (Codex review), applied here at USE time
    # since the renew target is read from stored/on-disk state rather than
    # validated once at enrollment: a nonlocal http:// target would send
    # the mTLS-authenticated renewal request in cleartext. renew() never
    # raises (see its docstring), so an insecure target is treated the same
    # as the "not http(s) at all" branch above -- logged, attempt recorded,
    # None returned.
    if not _url_passes_scheme_gate(target_base):
        logger.warning("renewal skipped: insecure renew target: %s", target_base)
        _record_renewal_attempt(identity, now)
        return None

    new_key_pem, new_csr_pem = generate_key_and_csr(common_name=identity.robot_id)

    body = json.dumps({"csr_pem": new_csr_pem.decode()}).encode()
    request = urllib.request.Request(  # noqa: S310 -- scheme enforced to http(s) above
        f"{target_base.rstrip('/')}/v1/renew",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    context = _build_mtls_context(identity)
    try:
        # urlopen raises HTTPError on non-2xx; context= is a no-op for the
        # http:// scheme a test's fake server uses, exercised for real only
        # against an https:// renew target in production.
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

    # The server is trusted to sign whatever CSR it was handed, but never
    # trusted to hand back a cert for a DIFFERENT key -- a server bug, a
    # stale/replayed response, or a mismatched-CSR mixup would otherwise get
    # committed as-is: identity.yaml would repoint at a cert nothing on disk
    # holds the matching private key for, breaking the next mTLS handshake
    # with no automatic recovery (Codex review). Reject before committing --
    # same typed-error path as the other malformed-response branches above:
    # no commit, old identity untouched, next daily attempt retries.
    if not _cert_matches_key(payload["cert_pem"], new_key_pem):
        logger.warning("renewal response cert_pem does not match this renewal's private key")
        _record_renewal_attempt(identity, now)
        return None

    return _commit_renewed_files(identity, new_key_pem, payload, now)


def renew(identity: Identity) -> Identity | None:
    """Renew ``identity``'s certificate: a NEW key + CSR, POSTed over mTLS.

    Generates a brand new private key and CSR -- not a re-use of the current
    key -- and POSTs it to ``{identity.renew_url or identity.enroll_url}/v1/renew``
    (see the module docstring: enroll and renew commonly live on different
    hosts) authenticated with the CURRENT cert/key pair over mTLS
    (``ssl.SSLContext`` + ``load_cert_chain``/``load_verify_locations``, see
    ``_build_mtls_context``). Per spec §6, renewal happens "with a new CSR";
    read strictly, a new CSR implies a new key backing it, so a compromised
    old key is never perpetuated across a renewal.

    On a 200 response, this uses identity.yaml's pointer scheme (see the
    module docstring) rather than overwriting the current key/cert/ca files
    in place: the new key (0600), cert, and (only if the response carries a
    ``ca_pem``) CA are all written under fresh, never-before-used basenames
    first; THEN one atomic identity.yaml write repoints
    ``key_file``/``cert_file``/``ca_file`` at them (with the new
    ``expires_at`` and ``last_renewal_attempt_at``) -- the single moment the
    renewal takes effect; THEN the now-superseded old files are unlinked,
    best-effort. An in-place overwrite instead would let a crash between the
    key and cert writes leave the pointer resolving to a mismatched
    new-key/old-cert pair FOREVER (``is_enrolled()`` still blesses it, and
    every subsequent renewal's ``_build_mtls_context`` fails the same way
    against the same broken pair) -- the pointer scheme makes that state
    unreachable: identity.yaml always names either the complete old pair or
    the complete new one, never a mix. On any failure below -- including a
    501/404 meaning the server does not offer renewal on this deployment yet
    -- the pointer is NEVER moved: only ``last_renewal_attempt_at`` changes
    in ``identity.yaml``, and the old key/cert/ca/``expires_at`` are left
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


def maybe_enroll_on_first_boot() -> None:
    """Enroll this robot at boot if a one-time token+URL are configured and it isn't yet.

    Called from server.py's ``__main__`` block, BEFORE ``startup.start()``, so
    a fresh enrollment's identity is already on disk by the time the
    manifest's ``streams:`` wiring looks for one (see ``src/sink/startup.py``).

    A no-op unless ``settings.FLEET_ENABLED`` is true (the kill switch's
    documented "makes the subsystem inert" contract -- Codex review: this
    was previously unchecked here, so ``FLEET_ENABLED=0`` did not stop a
    first-boot enrollment keygen+POST), AND both ``settings.FLEET_ENROLL_TOKEN``
    and ``settings.FLEET_ENROLL_URL`` are set AND
    ``settings.FLEET_IDENTITY_DIRECTORY`` is not already enrolled
    (``is_enrolled()``) -- an already-enrolled robot, or one with no token
    configured at all, does nothing here.

    Never raises: enrollment failing at boot (bad token, unreachable server,
    ...) must not brick the container -- it is logged at ERROR (the reason
    only; per ``enroll()``'s contract the token itself never appears in the
    exception or reaches this log line) and the server continues booting
    unenrolled. A successful enrollment logs the assigned tenant/robot --
    again, never the token.
    """
    logger = logging.getLogger(__name__)
    if not settings.FLEET_ENABLED:
        logger.debug(
            "First-boot fleet enrollment skipped: FLEET_ENABLED=0 (kill switch makes the "
            "fleet subsystem inert)"
        )
        return
    token = settings.FLEET_ENROLL_TOKEN
    url = settings.FLEET_ENROLL_URL
    if not token or not url:
        return
    directory = settings.FLEET_IDENTITY_DIRECTORY
    if is_enrolled(directory):
        return
    try:
        result = enroll(token, url, pathlib.Path(directory))
    except Exception as exc:
        logger.error("First-boot fleet enrollment failed: %s", exc)
        return
    logger.info(
        "First-boot fleet enrollment succeeded: tenant=%s robot=%s",
        result.tenant,
        result.robot_id,
    )
