"""A topic registry for InfluxDB 3 databases.

Each measurement (table) is a topic. Schemas come from a LIMIT-0 SQL query's Arrow
result, so types are exact -- InfluxDB 3 returns Arrow natively over Flight SQL.
"""

import pyarrow as pa

from bagel.di import module
from bagel.source.influxdb import InfluxDatabase
from bagel.topic import base


class TopicRegistry(base.TopicRegistry):
    """A topic registry for InfluxDB 3 databases."""

    def available_topics(self, data_source: InfluxDatabase) -> list[str]:
        """Return a list of available topic (measurement) names."""
        return data_source.tables()

    def _require_topic(self, topic: str, data_source: InfluxDatabase) -> None:
        if topic not in data_source.tables():
            raise base.TopicNotFoundError(topic)

    def native_type_name(self, topic: str, data_source: InfluxDatabase) -> str:
        """Return the native type name for the given topic."""
        self._require_topic(topic, data_source)
        return "influxdb/measurement"

    def message_count(self, topic: str, data_source: InfluxDatabase) -> int:
        """Return the number of rows for the given topic."""
        self._require_topic(topic, data_source)
        result = data_source.client.query(f'SELECT COUNT(*) AS n FROM "{topic}"')  # noqa: S608
        return result["n"][0].as_py()

    def struct(self, topic: str, data_source: InfluxDatabase) -> pa.StructType:
        """Return the PyArrow StructType for the given topic (all columns)."""
        self._require_topic(topic, data_source)
        empty = data_source.client.query(f'SELECT * FROM "{topic}" LIMIT 0')  # noqa: S608
        return pa.struct(empty.schema)

    def describe(self, topic: str, data_source: InfluxDatabase) -> str:
        """Return a human-readable description: the measurement's columns and types."""
        struct = self.struct(topic, data_source)
        lines = [f"measurement {topic} (timestamp column: time)"]
        lines += [f"  {field.name}: {field.type}" for field in struct]
        return "\n".join(lines)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicRegistry
