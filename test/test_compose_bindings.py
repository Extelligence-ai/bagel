"""Published ports must bind to localhost only (issue #158).

Docker publishes to 0.0.0.0 when the host side has no IP prefix, which exposes
the unauthenticated MCP endpoint (and Jupyter) to the whole LAN and bypasses
host firewalls like ufw. SECURITY.md's stated posture is localhost/trusted
network, so the compose file must match it by default.
"""

import pathlib
import re

import yaml

# Matches COPY of the local .env itself; deliberately does NOT match
# .env.example or other .env-prefixed sample files (Copilot on #163).
COPY_ENV_PATTERN = re.compile(r"\s*COPY\b.*[\s/]\.env(?=\s|$)")


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
    """Every MCP-publishing service must inject MCP_SERVER_HOST=0.0.0.0.

    The settings default is loopback (secure for bare host runs, Codex on
    #160), so containers must opt into all-interfaces explicitly or Docker
    port publishing cannot reach them.
    """
    offenders = []
    for name, service in _services().items():
        env = service.get("environment", {}) or {}
        if "MCP_SERVER_PORT" not in env:
            continue
        if str(env.get("MCP_SERVER_HOST", "")) != "0.0.0.0":  # noqa: S104 -- asserting config, not binding
            offenders.append(name)
    assert not offenders, f"services missing MCP_SERVER_HOST=0.0.0.0: {offenders}"


def test_host_run_defaults_to_loopback() -> None:
    """Bare host runs must not expose the unauthenticated endpoint."""
    env = pathlib.Path(".env").read_text(encoding="utf-8")
    assert "MCP_SERVER_HOST=127.0.0.1" in env


def test_every_mcp_publishing_service_passes_mcp_server_port_env() -> None:
    """Services that publish the MCP port must also inject MCP_SERVER_PORT.

    Settings defaults MCP_SERVER_PORT to 8000 (issue #158's Task 2), so a user
    who edits the port in .env but relies only on compose's `ports:` mapping
    gets a host-side mapping to their custom port while the in-container
    server keeps listening on 8000 -- connection refused. Every service whose
    `ports:` block maps `${MCP_SERVER_PORT}:${MCP_SERVER_PORT}` must also pass
    MCP_SERVER_PORT through its `environment:` block so the in-container
    server picks up the same value.
    """
    offenders = []
    for name, service in _services().items():
        mappings = [str(m) for m in service.get("ports", [])]
        publishes_mcp_port = any("${MCP_SERVER_PORT}" in m for m in mappings)
        if not publishes_mcp_port:
            continue
        env = service.get("environment", {}) or {}
        if str(env.get("MCP_SERVER_PORT", "")) != "${MCP_SERVER_PORT}":
            offenders.append(name)
    assert not offenders, (
        f"services publish the MCP port but don't pass MCP_SERVER_PORT in environment: {offenders}"
    )


def test_every_fleet_enabled_service_forwards_enrollment_settings() -> None:
    """Every service that gets FLEET_ENABLED must also get the enrollment settings.

    Codex review (round 2): compose.yaml forwarded FLEET_ENABLED but not
    FLEET_ENROLL_TOKEN/FLEET_ENROLL_URL, so `maybe_enroll_on_first_boot()`
    silently no-ops even when an operator exports both on the host --
    production mqtts:// startup fails with no obvious cause. Also requires
    FLEET_IDENTITY_DIRECTORY and FLEET_DEV_INSECURE passthrough, matching
    FLEET_ENABLED's own ``${VAR:-default}`` style.
    """
    required = {
        "FLEET_ENROLL_TOKEN": "${FLEET_ENROLL_TOKEN:-}",
        "FLEET_ENROLL_URL": "${FLEET_ENROLL_URL:-}",
        "FLEET_IDENTITY_DIRECTORY": "${FLEET_IDENTITY_DIRECTORY:-/home/ubuntu/.bagel/identity}",
        "FLEET_DEV_INSECURE": "${FLEET_DEV_INSECURE:-false}",
    }
    offenders = []
    for name, service in _services().items():
        env = service.get("environment", {}) or {}
        if "FLEET_ENABLED" not in env:
            continue
        for key, expected in required.items():
            if str(env.get(key, "")) != expected:
                offenders.append(f"{name}: {key}")
    assert not offenders, f"FLEET_ENABLED services missing enrollment passthrough: {offenders}"


def test_every_fleet_enabled_service_persists_identity_directory() -> None:
    """The identity directory must be mounted from the host, not left in the container layer.

    Codex review (round 2): with the documented `docker compose run`
    workflow, an unmounted identity directory lives only in the one-off
    container's writable layer and is lost on exit; because
    FLEET_ENROLL_TOKEN is single-use, a later run can't re-enroll to replace
    it. The host-side mount must match FLEET_IDENTITY_DIRECTORY's
    container-side default (/home/ubuntu/.bagel/identity) set above.
    """
    expected = "${HOME}/.bagel/identity:/home/ubuntu/.bagel/identity"
    offenders = []
    for name, service in _services().items():
        env = service.get("environment", {}) or {}
        if "FLEET_ENABLED" not in env:
            continue
        volumes = [str(v) for v in service.get("volumes", [])]
        if expected not in volumes:
            offenders.append(name)
    assert not offenders, f"FLEET_ENABLED services missing a persistent identity mount: {offenders}"


def test_no_dockerfile_copies_env_file() -> None:
    """Published images must never bake in the local .env (see SECURITY.md).

    Task 2 (#158) removed the `COPY .env` that used to embed local secrets
    into public image layers. This guards against it silently coming back in
    a future Dockerfile edit or a new Dockerfile.*.
    """
    offenders = []
    for dockerfile in sorted(pathlib.Path("docker").glob("Dockerfile.*")):
        for lineno, line in enumerate(dockerfile.read_text(encoding="utf-8").splitlines(), start=1):
            if COPY_ENV_PATTERN.match(line):
                offenders.append(f"{dockerfile}:{lineno}: {line.strip()}")
    assert not offenders, f"Dockerfile(s) copy .env into the image: {offenders}"
