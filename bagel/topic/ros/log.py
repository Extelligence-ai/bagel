"""A topic registry for plain-text ROS log files.

Each node that logged at least one message becomes a topic, so SQL can filter
by node the same way it filters by topic on a bag.
"""

import pyarrow as pa

from bagel.di import module
from bagel.source.ros.log import RosLogSource
from bagel.topic import base

NATIVE_TYPE_NAME = "ros/log"

STRUCT = pa.struct(
    [
        pa.field(
            "level",
            pa.string(),
            nullable=False,
            metadata={"description": "log severity: DEBUG, INFO, WARN, ERROR, or FATAL"},
        ),
        pa.field(
            "message",
            pa.string(),
            nullable=False,
            metadata={"description": "log message text; multi-line for tracebacks"},
        ),
        pa.field(
            "file",
            pa.string(),
            nullable=False,
            metadata={"description": "name of the log file the record came from"},
        ),
    ]
)


class TopicRegistry(base.TopicRegistry):
    """A topic registry for plain-text ROS log files."""

    def available_topics(self, data_source: RosLogSource) -> list[str]:
        """Return the nodes that logged at least one message."""
        return sorted({record.node for record in data_source.records})

    def native_type_name(self, topic: str, data_source: RosLogSource) -> str:
        """Return the native type name for the given topic."""
        self._check_topic(topic, data_source)
        return NATIVE_TYPE_NAME

    def message_count(self, topic: str, data_source: RosLogSource) -> int | None:
        """Return the number of log records for the given node."""
        self._check_topic(topic, data_source)
        return sum(1 for record in data_source.records if record.node == topic)

    def struct(self, topic: str, data_source: RosLogSource) -> pa.StructType:
        """Return the PyArrow StructType for the given topic."""
        self._check_topic(topic, data_source)
        return STRUCT

    def describe(self, topic: str, data_source: RosLogSource) -> str | None:
        """Return a human-readable description of the given topic."""
        self._check_topic(topic, data_source)
        counts: dict[str, int] = {}
        for record in data_source.records:
            if record.node == topic:
                counts[record.level] = counts.get(record.level, 0) + 1
        breakdown = ", ".join(f"{count} {level}" for level, count in sorted(counts.items()))
        return f"Text log messages from ROS node '{topic}' ({breakdown})."

    def _check_topic(self, topic: str, data_source: RosLogSource) -> None:
        if topic not in self.available_topics(data_source):
            raise base.TopicNotFoundError(topic)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
