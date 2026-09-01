"""Connection policy: turn stream config + fleet identity into MqttPublisher kwargs.

One function, ``resolve_publisher_kwargs``, decides two things every time a
fleet publisher is (re)built: which broker to dial, and whether the
transport is allowed. ``mqtts://`` is the only production path -- it always
requires an enrolled identity, whose cert/key paths become the TLS kwargs.
``mqtt://`` (plaintext) exists only for local development: it requires both
the ``FLEET_DEV_INSECURE`` escape hatch *and* a broker host that resolves to
loopback or a private (RFC1918/RFC4193) address, so a misconfigured
production robot can never silently fall back to an unauthenticated,
unencrypted broker on the public internet.

The returned dict is exactly the kwargs ``MqttPublisher(**kwargs)`` accepts
(``broker_url``, ``tenant``, ``robot``, and -- for ``mqtts://`` -- the three
``tls_*`` paths); ``tenant``/``robot`` live in that same dict for a caller
that also needs them separately (e.g. ``Spool.for_robot(f"{tenant}/{robot}")``,
which wants the combined ``Identity.robot`` form rather than these two
split fields).

``identity``, ``StreamsConfig``, and ``settings`` are imported only inside
functions (or under ``TYPE_CHECKING``) so importing this module carries no
weight beyond the stdlib -- consistent with the rest of this package, which
imports paho/cryptography only where they are actually used.
"""

from __future__ import annotations

import ipaddress
import socket
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from src.sink.publish import FleetNotEnrolledError, StreamConfigError

if TYPE_CHECKING:
    from src.sink.publish.config import StreamsConfig
    from src.sink.publish.identity import Identity

_DEV_TENANT = "dev"
_DEV_ROBOT = "robot"


def _resolved_addresses(host: str) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    """Resolve ``host`` via ``socket.getaddrinfo``; ``[]`` on failure or no results.

    A real DNS lookup at call time -- tests must monkeypatch ``socket.getaddrinfo``,
    never let this hit the network.
    """
    try:
        results = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return []
    try:
        return [
            ipaddress.ip_address(sockaddr[0]) for _fam, _typ, _proto, _canon, sockaddr in results
        ]
    except ValueError:
        # A resolved "address" that ipaddress can't parse can't be confirmed
        # private -- fail closed, same as a resolution failure.
        return []


def _is_local_or_private(host: str) -> bool:
    """Return whether ``host`` names a loopback or private (non-routable) address.

    ``"localhost"`` is accepted by name without resolution. A literal IPv4 or
    IPv6 address is checked directly via ``ipaddress`` (loopback or private).
    Any other hostname is resolved with ``socket.getaddrinfo`` (see
    ``_resolved_addresses``), and ALL returned addresses must be
    loopback/private for the host to pass, so a DNS answer mixing a private
    and a public address is rejected rather than trusted on its best entry.
    A resolution failure or an empty result list returns False (fail closed:
    an unresolvable host is never treated as private).
    """
    if host == "localhost":
        return True
    try:
        literal = ipaddress.ip_address(host)
        addresses = [literal]
    except ValueError:
        addresses = _resolved_addresses(host)
    return bool(addresses) and all(a.is_loopback or a.is_private for a in addresses)


def resolve_publisher_kwargs(streams: StreamsConfig, identity: Identity | None) -> dict:
    """Decide the broker and auth for a fleet publisher; return MqttPublisher kwargs.

    Broker precedence: ``streams.broker`` (from the manifest) wins if set,
    else ``identity.broker_url`` (assigned by the enrollment server). If
    neither is available, raises ``FleetNotEnrolledError`` naming both the
    manifest setting and the enrollment path as remedies.

    ``mqtts://`` requires ``identity``: the returned kwargs carry
    ``tenant``/``robot`` from it and TLS material (``tls_ca_certs``,
    ``tls_certfile``, ``tls_keyfile``) from its stored cert paths. No
    anonymous TLS -- an ``mqtts://`` broker with no identity always raises.

    ``mqtt://`` (plaintext) requires ``settings.FLEET_DEV_INSECURE`` to be
    true (else a ``StreamConfigError`` naming that setting) AND the broker
    host to resolve as loopback/private per ``_is_local_or_private`` (else a
    ``StreamConfigError`` naming the host). Identity is optional here: with
    one, ``tenant``/``robot`` come from it same as ``mqtts://``; without one,
    this is a dev-only, unenrolled robot and the kwargs use the documented
    placeholder identity ``tenant="dev"``, ``robot="robot"``.

    Any other broker scheme raises ``StreamConfigError`` naming the scheme.
    ``StreamsConfig.build`` constrains ``streams.broker`` to ``mqtt``/``mqtts``,
    but ``load_identity``/``enroll`` do NOT validate ``broker_url``'s scheme --
    an ``identity.yaml`` written by a compromised or misbehaving enrollment
    server (or hand-edited on disk) can carry any scheme at all. This branch
    is that guard, not dead code: it is the last line of defense against a
    malicious ``broker_url`` reaching ``MqttPublisher`` unchecked.

    Returns:
        A dict of exactly the kwargs ``MqttPublisher(**kwargs)`` accepts.

    Raises:
        FleetNotEnrolledError: no broker configured, or ``mqtts://`` chosen
            with no enrolled identity.
        StreamConfigError: ``mqtt://`` chosen without ``FLEET_DEV_INSECURE``,
            or with a host that is not loopback/private, or an unsupported
            broker scheme.

    """
    from settings import settings

    broker_url = streams.broker or (identity.broker_url if identity is not None else None)
    if not broker_url:
        raise FleetNotEnrolledError(
            "no fleet broker configured: set 'streams.broker' in the manifest, "
            "or enroll this robot (identity.enroll) so 'identity.broker_url' is "
            "available"
        )

    scheme = urlparse(broker_url).scheme

    if scheme == "mqtts":
        if identity is None:
            raise FleetNotEnrolledError(
                f"mqtts://{urlparse(broker_url).hostname} requires an enrolled fleet "
                "identity for TLS material and tenant/robot; enroll this robot "
                "(identity.enroll) first"
            )
        return {
            "broker_url": broker_url,
            "tenant": identity.tenant,
            "robot": identity.robot_id,
            "tls_ca_certs": str(identity.ca_path),
            "tls_certfile": str(identity.cert_path),
            "tls_keyfile": str(identity.key_path),
        }

    if scheme == "mqtt":
        if not settings.FLEET_DEV_INSECURE:
            raise StreamConfigError(
                "streams.broker",
                f"plaintext mqtt:// broker {broker_url!r} requires "
                "FLEET_DEV_INSECURE=1 (local development only); use mqtts:// "
                "with an enrolled identity otherwise",
            )
        host = urlparse(broker_url).hostname
        if host is None or not _is_local_or_private(host):
            raise StreamConfigError(
                "streams.broker",
                f"mqtt:// host {host!r} does not resolve to loopback/private: "
                "FLEET_DEV_INSECURE only permits a local or private-network broker",
            )
        if identity is not None:
            tenant, robot = identity.tenant, identity.robot_id
        else:
            # Dev-only, unenrolled robot: a fixed placeholder identity, never
            # used for anything but a local mqtt:// broker gated above.
            tenant, robot = _DEV_TENANT, _DEV_ROBOT
        return {"broker_url": broker_url, "tenant": tenant, "robot": robot}

    raise StreamConfigError(
        "streams.broker", f"unsupported broker scheme {scheme!r}: {broker_url!r}"
    )
