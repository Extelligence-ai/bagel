"""Run a SQL query on topic messages at a given time to determine if gating criteria is met."""

import duckdb

from src.di import module
from src.pipeline import base, messages


class SqlQueryGate(base.TopicMessageMixin, base.Gate):
    """Run a SQL query on topic messages at a given time to determine if gating criteria is met."""

    def __init__(self, topic: str, statement: str) -> None:
        """Initialize the gate.

        Args:
            topic (str): The topic to run the SQL query on.
            statement (str): The SQL query to execute. The `FROM` clause must refer to
                the topic name as a table. It must return a **single** boolean value to
                indicate whether the gating criteria is met.

        """
        self._topic = topic
        self._statement = statement

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        """Evaluate whether the gating criteria is met at the given time."""
        relation = messages.to_duckdb(
            factory=self.factory,
            registry=self.registry,
            dataset=self.dataset,
            topics=[self._topic],
            asof_seconds=asof_seconds,
            lookback=lookback,
        )
        duckdb.register(self._topic, relation)
        result = duckdb.sql(self._statement).fetchall()
        if len(result) != 1:
            raise ValueError(f"SQL query must return one row, got {len(result)} rows")
        elif len(result[0]) != 1:
            raise ValueError(
                f"SQL query must return one row with one column, got {len(result[0])} columns"
            )
        elif not isinstance(result[0][0], bool):
            raise ValueError(f"SQL query must return a boolean value, got {type(result[0][0])}")
        duckdb.unregister(self._topic)
        return bool(result[0][0])


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SqlQueryGate
