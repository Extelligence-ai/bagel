"""Fleet streaming: publish live channels, events and heartbeats to a fleet broker.

This package is optional. ``require_fleet()`` is the single gate every fleet
entry point calls first; it never imports the MQTT client at module import
time so the rest of Bagel does not depend on it.
"""

from settings import settings

INSTALL_HINT = (
    "Fleet streaming is not installed in this image: the `fleet` dependency group "
    "(paho-mqtt) is missing. Use an image built with `--group fleet`, or run "
    "`uv sync --group fleet` in a source checkout."
)


class FleetNotInstalledError(Exception):
    """Raised when the optional `fleet` dependency group is not installed."""


class FleetDisabledError(Exception):
    """Raised when FLEET_ENABLED=0 has switched the publish subsystem off."""


class StreamConfigError(Exception):
    """Raised when the manifest's `streams:` section fails load-time validation."""

    def __init__(self, field: str, reason: str) -> None:
        """Initialize StreamConfigError with field and reason."""
        self.field = field
        self.reason = reason
        super().__init__(f"streams config: {field}: {reason}")


def require_fleet() -> None:
    """Check the kill switch, then that the MQTT client is importable.

    Raises:
        FleetDisabledError: if ``settings.FLEET_ENABLED`` is false.
        FleetNotInstalledError: if ``paho`` cannot be imported.

    """
    if not settings.FLEET_ENABLED:
        raise FleetDisabledError(
            "Fleet streaming is disabled on this server (FLEET_ENABLED=0). "
            "Set FLEET_ENABLED=1 and restart to use fleet tools."
        )
    try:
        import paho.mqtt.client  # noqa: F401  (deferred: optional dependency)
    except ImportError as exc:
        raise FleetNotInstalledError(INSTALL_HINT) from exc
