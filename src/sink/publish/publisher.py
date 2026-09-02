"""Publisher interface for fleet streaming (spec §2/§3).

Concrete implementations own the transport; the interface owns the wire
contract: topic namespace, QoS-1 semantics, and which kinds are retained.
Payloads arrive as ready dicts — batching and sequencing live upstream.
"""

import abc

KINDS = ("schema", "channels", "events", "heartbeat", "cmd")
LWT_PAYLOAD = {"v": 1, "online": False, "reason": "lwt"}


class PublishError(Exception):
    """Raised when a QoS-1 publish is not acknowledged."""


def wire_topic(tenant: str, robot: str, kind: str) -> str:
    """Return the v1 wire topic for one message kind of one robot."""
    if kind not in KINDS:
        raise ValueError(f"unknown kind '{kind}'; expected one of {KINDS}")
    return f"bagel/v1/{tenant}/{robot}/{kind}"


class Publisher(abc.ABC):
    """One robot's session to a fleet broker."""

    @abc.abstractmethod
    def connect(self) -> None:
        """Connect to the broker."""

    @abc.abstractmethod
    def publish(
        self,
        kind: str,
        payload: dict,
        *,
        retain: bool = False,
        timeout_s: float = 10.0,
    ) -> None:
        """Publish a message to the broker."""

    @abc.abstractmethod
    def close(self, reason: str = "stopped") -> None:
        """Close the broker connection.

        Args:
            reason: Carried in the clean-stop heartbeat payload by
                implementations that publish one before disconnecting (see
                `MqttPublisher.close`) -- spec §3's `{"online": false,
                "reason": ...}` shape. Defaults to `"stopped"`;
                `FleetService.pause()` passes `"paused"` so a paused robot's
                last-known-state is distinguishable from a genuinely stopped
                one.

        """

    @property
    @abc.abstractmethod
    def connected(self) -> bool:
        """Return whether the broker is connected."""

    def publish_schema(self, payload: dict) -> None:
        """Publish a schema message (retained)."""
        self.publish("schema", payload, retain=True)

    def publish_heartbeat(self, payload: dict) -> None:
        """Publish a heartbeat message (retained)."""
        self.publish("heartbeat", payload, retain=True)

    def publish_channels(self, payload: dict) -> None:
        """Publish a channels message (not retained)."""
        self.publish("channels", payload)

    def publish_event(self, payload: dict) -> None:
        """Publish an event message (not retained)."""
        self.publish("events", payload)
