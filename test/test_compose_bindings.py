"""Published ports must bind to localhost only (issue #158).

Docker publishes to 0.0.0.0 when the host side has no IP prefix, which exposes
the unauthenticated MCP endpoint (and Jupyter) to the whole LAN and bypasses
host firewalls like ufw. SECURITY.md's stated posture is localhost/trusted
network, so the compose file must match it by default.
"""

import pathlib

import yaml


def _services() -> dict:
    compose = yaml.safe_load(pathlib.Path("compose.yaml").read_text(encoding="utf-8"))
    return compose["services"]


def test_every_published_port_binds_to_localhost() -> None:
    offenders = []
    for name, service in _services().items():
        for mapping in service.get("ports", []):
            if not str(mapping).startswith("127.0.0.1:"):
                offenders.append(f"{name}: {mapping}")
    assert not offenders, f"ports published to all interfaces: {offenders}"


def test_container_side_host_still_binds_all_interfaces() -> None:
    """The in-container listen address must stay 0.0.0.0 for port mapping to work."""
    env = pathlib.Path(".env").read_text(encoding="utf-8")
    assert "MCP_SERVER_HOST=0.0.0.0" in env
