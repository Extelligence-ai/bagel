"""Fleet control-plane operations: the tool-facing layer over `FleetService`.

Step 7's MCP tools are thin passthroughs to this module (spec §7) -- this is
where the four-state sanity ruling lives: every operation here behaves
sanely whether the `fleet` dependency group is installed, whether
`settings.FLEET_ENABLED` is on, whether this robot is enrolled, and whether a
`FleetService` is currently running.

`fleet_status()` is the one exception to `require_fleet()`: per the binding
ruling, it calls NO gate at all, because it IS the tool that reports on
those first two states rather than assuming them -- a caller diagnosing "why
won't fleet streaming start" needs an answer even when it's not installed or
disabled, not an exception. `pause_streaming`/`resume_streaming` call
`require_fleet()` first, so `FLEET_ENABLED=0` raises `FleetDisabledError`
before the live-service holder (`src.sink.startup.fleet_service()`) is ever
touched -- there is no code path where a disabled fleet subsystem still
reaches into the holder.

Only `startup` (the live-service holder) and `identity` (enrollment status)
are needed for the operations this module carries today (status/pause/
resume); later tasks in this step add `enroll_identity`/`unenroll_identity`/
`stream_topics`/`stop_streams`, which pull in `connect`, `config`, `spool`,
and `mqtt` too -- all of those stay just as lazy about paho/cryptography as
this module is (neither is imported here at module scope).
"""

import importlib.util

from settings import settings
from src.sink import startup
from src.sink.publish import identity, require_fleet


def fleet_status() -> dict:
    """Report this robot's fleet-streaming state -- the local source of truth (spec §7).

    Calls NO gate: this must answer sanely in every state (uninstalled,
    disabled, unenrolled, no service running), not raise when one of those
    is exactly what the caller is trying to diagnose.

    Returns:
        ```
        {
          "enabled": bool,        # settings.FLEET_ENABLED
          "installed": bool,      # the `fleet` dependency group (paho-mqtt) is importable
          "enrolled": bool,       # a complete identity is stored on disk
          "identity": {"tenant", "robot_id", "broker_url", "cert_expires_at",
                       "renew_url"} | None,  # never key material, never paths
          "service": "running" | "paused" | "stopped",
          "channels": list[dict],  # resolved channel descriptors, [] if no service
          "status": dict | None,   # FleetService.status()'s §4 counters block, verbatim
        }
        ```

    """
    service = startup.fleet_service()
    if service is None:
        service_state = "stopped"
    elif service.paused:
        service_state = "paused"
    else:
        service_state = "running"

    enrolled = identity.is_enrolled(settings.FLEET_IDENTITY_DIRECTORY)
    identity_summary = None
    if enrolled:
        loaded = identity.load_identity(settings.FLEET_IDENTITY_DIRECTORY)
        identity_summary = {
            "tenant": loaded.tenant,
            "robot_id": loaded.robot_id,
            "broker_url": loaded.broker_url,
            "cert_expires_at": loaded.expires_at,
            "renew_url": loaded.renew_url,
        }

    return {
        "enabled": bool(settings.FLEET_ENABLED),
        "installed": importlib.util.find_spec("paho.mqtt") is not None,
        "enrolled": enrolled,
        "identity": identity_summary,
        "service": service_state,
        "channels": service.channels if service is not None else [],
        "status": service.status() if service is not None else None,
    }


def pause_streaming(discard: bool = False) -> dict:
    """Pause the live `FleetService`, if any: go offline, keep identity + rules.

    `require_fleet()` first -- `FLEET_ENABLED=0` raises `FleetDisabledError`
    before the holder is touched at all.

    Args:
        discard: When true, additionally empties the channels spool lane's
            still-unacked backlog (events/heartbeat are never-drop lanes and
            are left untouched -- see `FleetService.pause`).

    Returns:
        `{"service": "stopped", "changed": False}` when no service is
        running (an idempotent no-op). Otherwise `{"service": "paused",
        "changed": bool, "discarded": discard}`, where `changed` is `True`
        only when this call actually transitioned a running service to
        paused (a second `pause_streaming()` call on an already-paused
        service reports `changed: False`).

    Raises:
        FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.

    """
    require_fleet()
    service = startup.fleet_service()
    if service is None:
        return {"service": "stopped", "changed": False}
    changed = not service.paused
    service.pause(discard=discard)
    return {"service": "paused", "changed": changed, "discarded": discard}


def resume_streaming() -> dict:
    """Resume the live `FleetService`, if any: mirror image of `pause_streaming`.

    `require_fleet()` first -- `FLEET_ENABLED=0` raises `FleetDisabledError`
    before the holder is touched at all.

    Returns:
        `{"service": "stopped", "changed": False}` when no service is
        running (an idempotent no-op). Otherwise `{"service": "running",
        "changed": bool}`, where `changed` is `True` only when this call
        actually transitioned a paused service back to running (calling
        this on an already-running service reports `changed: False`).

    Raises:
        FleetDisabledError | FleetNotInstalledError: via `require_fleet()`.

    """
    require_fleet()
    service = startup.fleet_service()
    if service is None:
        return {"service": "stopped", "changed": False}
    changed = service.paused
    service.resume()
    return {"service": "running", "changed": changed}
