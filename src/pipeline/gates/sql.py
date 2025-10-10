"""A gate that evaluates whether a SQL statement passes on messages."""

from typing import Any

import duckdb

from src.di import module
from src.message.base import MessageDataset
from src.pipeline import base, messages
from src.source.base import SourceFactory
from src.topic.base import TopicRegistry


class Gate(base.Gate):
    """A gate that evaluates whether a SQL statement passes on messages."""

    def __init__(  # noqa: PLR0913
        self,
        factory: SourceFactory,
        registry: TopicRegistry,
        dataset: MessageDataset,
        topic: str,
        statement: str,
        last: int | None = None,
        unit: str | None = None,
    ) -> None:
        """Initialize a SQL gate.

        Args:
            factory (SourceFactory): A data source factory.
            registry (TopicRegistry): A topic registry.
            dataset (MessageDataset): A message dataset.
            topic (str): The topic to evaluate.
            statement (str): The SQL statement to evaluate. The statement **must** return a single
                boolean value.
            last (int | None, optional): Value of the lookback window. Defaults to None.
            unit (str | None, optional): The unit of the lookback window. Defaults to None.

        """
        self._factory = factory
        self._registry = registry
        self._dataset = dataset
        self._topic = topic
        self._statement = statement
        self._lookback = base.Lookback.build(last, unit)

    def evaluate(self, asof_seconds: float) -> bool:
        """Evaluate if the SQL statement is true at the given time."""
        relation = messages.to_duckdb(
            factory=self._factory,
            registry=self._registry,
            dataset=self._dataset,
            topics=[self._topic],
            asof_seconds=asof_seconds,
            lookback=self._lookback,
        )
        duckdb.register(self._topic, relation)
        result = duckdb.sql(self._statement).fetchall()
        if len(result) != 1:
            raise ValueError(f"SQL statement must return exactly one row, got {len(result)} rows")
        elif len(result[0]) != 1:
            raise ValueError(
                f"SQL statement must return exactly one row with one column, got {len(result[0])} columns"  # noqa: E501
            )
        elif not isinstance(result[0][0], bool):
            raise ValueError(f"SQL statement must return a boolean value, got {type(result[0][0])}")
        duckdb.unregister(self._topic)
        return result[0][0]

    @staticmethod
    def build(args: dict[str, Any]) -> "Gate":
        """Build a gate from configuration."""
        factory = module.provide(args["factory"]["module"], args["factory"].get("args", {}))
        registry = module.provide(args["registry"]["module"], args["registry"].get("args", {}))
        dataset = module.provide(args["dataset"]["module"], args["dataset"].get("args", {}))
        return Gate(
            factory=factory,
            registry=registry,
            dataset=dataset,
            topic=args["topic"],
            statement=args["statement"],
            last=args.get("last"),
            unit=args.get("unit"),
        )


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = Gate
