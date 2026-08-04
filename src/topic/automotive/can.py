"""A topic registry for DBC-decoded CAN logs: DBC messages are topics."""

import pyarrow as pa

from src.di import module
from src.source.automotive.can import CanLog
from src.topic import base

NATIVE_TYPE_NAME = "can/dbc_message"


class TopicRegistry(base.TopicRegistry):
    """A topic registry for DBC-decoded CAN logs."""

    def available_topics(self, data_source: CanLog) -> list[str]:
        """Return the DBC message names observed in the capture."""
        return sorted(data_source.topic_message_counts)

    def native_type_name(self, topic: str, data_source: CanLog) -> str:
        """Return the native type name for the given topic."""
        self._message(topic, data_source)
        return NATIVE_TYPE_NAME

    def message_count(self, topic: str, data_source: CanLog) -> int | None:
        """Return the number of decoded frames for the message."""
        self._message(topic, data_source)
        return data_source.topic_message_counts.get(topic, 0)

    def struct(self, topic: str, data_source: CanLog) -> pa.StructType:
        """Return one float field per DBC signal, with units attached as metadata."""
        message = self._message(topic, data_source)
        return pa.struct(
            [
                pa.field(
                    signal.name,
                    pa.float64(),
                    metadata={
                        "description": signal.comment or f"DBC signal '{signal.name}'",
                        "units": signal.unit or "",
                    },
                )
                for signal in message.signals
            ]
        )

    def describe(self, topic: str, data_source: CanLog) -> str | None:
        """Return a human-readable description of the DBC message."""
        message = self._message(topic, data_source)
        signals = ", ".join(f"{s.name} ({s.unit})" if s.unit else s.name for s in message.signals)
        comment = message.comment or f"CAN message 0x{message.frame_id:X}"
        return f"{comment}. Signals: {signals}."

    def _message(self, topic: str, data_source: CanLog) -> object:
        try:
            return data_source.database.get_message_by_name(topic)
        except KeyError as error:
            raise base.TopicNotFoundError(topic) from error


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
