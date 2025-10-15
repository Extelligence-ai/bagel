"""Base classes and utilities for defining and running data processing pipelines."""

import abc
import importlib
import logging
import pathlib
from collections.abc import Iterator
from enum import Enum
from typing import Any

import yaml
from pydantic import BaseModel

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import DataSource, resolve
from src.image.base import ImageDataset
from src.message.base import MessageDataset
from src.source.base import SourceFactory
from src.topic.base import TopicRegistry

SECOND = 1
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE


class MissingRequiredKeyError(Exception):
    """Raised when a required key is missing in the pipeline YAML configuration."""


class Unit(Enum):
    """Unit of a cadence interval."""

    FRAME = "frame"
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"


def to_seconds(value: int, unit: Unit) -> float:
    """Cast a value in the given unit to seconds."""
    match unit:
        case Unit.SECOND:
            return value * SECOND
        case Unit.MINUTE:
            return value * MINUTE
        case Unit.HOUR:
            return value * HOUR
        case _:
            raise ValueError(f"Cannot convert {unit} to seconds")


class OnceAtEnd(BaseModel):
    """A `cadence.when` option that triggers the pipeline at the natural end of the data source.

    Examples:
        - For a bounded source (e.g., a ROS 2 bag), the pipeline runs once at the final timestamp.
        - For a live stream, the pipeline runs once when the stream terminates.

    """


class Frequency(BaseModel):
    """A `cadence.when` option that triggers the pipeline at a fixed frequency.

    Examples:
        - Every 5 minutes.
        - Every 30 frames.

    """

    every: int
    unit: Unit

    def to_seconds(self) -> float:
        """Convert the frequency interval to seconds, if applicable."""
        return to_seconds(self.every, self.unit)


class Lookback(BaseModel):
    """A lookback window defining how far back in time to consider data for gates and tasks.

    Examples:
        - The last 10 frames.
        - The last 5 minutes.

    """

    last: int
    unit: Unit

    def to_seconds(self) -> float:
        """Convert the lookback window to seconds, if applicable."""
        return to_seconds(self.last, self.unit)

    @staticmethod
    def build(config: dict[str, Any]) -> "Lookback":
        """Build a Lookback instance from the value of a 'lookback' key."""
        match config:
            case {"last": int(last), "unit": str(unit)}:
                return Lookback(last=last, unit=Unit(unit))
            case _:
                raise ValueError(f"Invalid 'lookback' value: {config}")


class Cadence(BaseModel):
    """Specify how often the pipeline runs and which topic determines its cadence.

    Examples:
        - Run the pipeline once every 30 frames of the `/camera/image` topic.
        - Run the pipeline once at the final timestamp of the `/lidar/points` topic.

    """

    topic: str
    when: OnceAtEnd | Frequency

    @staticmethod
    def build(config: dict[str, Any]) -> "Cadence":
        """Build a Cadence instance from the value of a 'cadence' key."""
        match config["when"]:
            case "once_at_end":
                when = OnceAtEnd()
            case {"every": int(every), "unit": str(unit)}:
                when = Frequency(every=every, unit=Unit(unit))
            case when:
                raise ValueError(f"Invalid 'when' value: {when}")
        return Cadence(topic=config["topic"], when=when)


class Operator(abc.ABC):
    """Abstract base class for gate and task operators."""

    @abc.abstractmethod
    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Initialize the operator using the given data source.

        Called immediately after construction to set up any resources that depend on
        the data source, such as a SourceFactory, TopicRegistry, etc.

        Args:
            path: Filesystem path or URL to the data source.
            **kwargs: Additional parameters for setup.

        """

    @staticmethod
    def build(path: str, config: dict[str, Any]) -> "Operator":
        """Build an Operator instance from a config dictionary of a gate or task.

        Args:
            path (str): Filesystem path or URL to the data source.
            config (dict[str, Any]): Configuration dictionary for the operator.

        Returns:
            Operator: The constructed Operator instance.

        """
        importlib.import_module(config["module"]).register()
        cls = module.global_registry[config["module"]]
        instance: Operator = cls(**config.get("args", {}))
        instance.setup(path=path, **config.get("setup_args", {}))
        return instance


class TopicMessageMixin:
    """Mixin for operators that work on messages in a topic."""

    _factory: SourceFactory
    _registry: TopicRegistry
    _dataset: MessageDataset

    @property
    def factory(self) -> SourceFactory:  # noqa: D102
        return self._factory

    @property
    def registry(self) -> TopicRegistry:  # noqa: D102
        return self._registry

    @property
    def dataset(self) -> MessageDataset:  # noqa: D102
        return self._dataset

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003, D102
        ds_type = resolve(path)

        factory_module = f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}"
        registry_module = f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}"
        dataset_module = f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}"

        self._factory = module.provide(factory_module, {"path": path, **kwargs})
        self._registry = module.provide(registry_module, {**kwargs})
        self._dataset = module.provide(dataset_module, {**kwargs})


class TopicImageMixin:
    """Mixin for operators that work on images in a topic."""

    _factory: SourceFactory
    _registry: TopicRegistry
    _dataset: ImageDataset

    @property
    def factory(self) -> SourceFactory:  # noqa: D102
        return self._factory

    @property
    def registry(self) -> TopicRegistry:  # noqa: D102
        return self._registry

    @property
    def dataset(self) -> ImageDataset:  # noqa: D102
        return self._dataset

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003, D102
        ds_type = resolve(path)

        factory_module = f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}"
        registry_module = f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}"
        dataset_module = None
        match ds_type:
            case DataSource.ROS1_BAG:
                dataset_module = f"{BaseModule.IMAGE_DATASET.value}.ros1.bag"
            case DataSource.BAGEL_SINK:
                metadata_file = pathlib.Path(path) / "metadata.yaml"
                metadata = yaml.safe_load(metadata_file.read_text())
                dataset_module = metadata.get("image_dataset_module")
        if dataset_module is None:
            raise ValueError(f"{ds_type} not supported")

        self._factory = module.provide(factory_module, {"path": path, **kwargs})
        self._registry = module.provide(registry_module, {**kwargs})
        self._dataset = module.provide(dataset_module, {**kwargs})


class Gate(Operator):
    """Abstract base class for gating operators.

    A gate determines whether downstream tasks should be executed based on evaluated criteria.

    """

    @abc.abstractmethod
    def evaluate(self, asof_seconds: float, lookback: Lookback | None) -> bool:
        """Evaluate the gating criteria at a specific point in time.

        Args:
            asof_seconds (float): The timestamp (in seconds) at which to evaluate the gate.
            lookback (Lookback | None): The lookback window defining how far back to consider data.
                If None, all available data up to `asof_seconds` should be considered. Some gates
                may not support lookback and will ignore this argument.

        Returns:
            True if the gate criteria is met.

        """


class Task(Operator):
    """Abstract base class for task operators.

    A task performs a specific action when executed, such as sending an email or generating a GIF.

    """

    @abc.abstractmethod
    def execute(self, asof_seconds: float, lookback: Lookback | None) -> None:
        """Execute the task at a specific point in time.

        Args:
            asof_seconds (float): The timestamp (in seconds) at which to execute the task.
            lookback (Lookback | None): The lookback window defining how far back to consider data.
                If None, all available data up to `asof_seconds` should be considered. Some tasks
                may not support lookback and will ignore this argument.

        """


class Pipeline:
    """A data processing pipeline consisting of gates and tasks.

    It runs at specified intervals based on a cadence, evaluates gates,
    and executes tasks if the gates pass.

    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        path: str,
        allow_failure: bool,
        cadence: Cadence,
        gates: list[tuple[Gate, Lookback | None]],
        tasks: list[tuple[Task, Lookback | None]],
    ) -> None:
        """Initialize a Pipeline instance.

        Args:
            name (str): The name of the pipeline.
            path (str): Filesystem path or URL to the data source.
            allow_failure (bool): Whether to continue executing pipeline runs if a run fails.
            cadence (Cadence): How often to run the pipeline.
            gates (list[tuple[Gate, Lookback  |  None]]): List of gating operators and their
                lookback windows.
            tasks (list[tuple[Task, Lookback  |  None]]): List of task operators and their
                lookback windows.

        """
        self._name = name
        self._path = path
        self._allow_failure = allow_failure
        self._cadence = cadence
        self._gates = gates
        self._tasks = tasks

    @property
    def name(self) -> str:
        """The name of the pipeline."""
        return self._name

    @property
    def cadence(self) -> Cadence:
        """How often to run the pipeline."""
        return self._cadence

    def _asof_timestamps(self) -> Iterator[float]:
        ds_type = resolve(self._path)

        factory = module.provide(
            f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": self._path}
        )
        registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
        dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})

        relation = dataset.to_duckdb(
            factory=factory,
            registry=registry,
            topics=[self.cadence.topic],
            start_seconds=None,
            end_seconds=None,
        )
        rows = relation.project(settings.TIMESTAMP_SECONDS_COLUMN_NAME).fetchall()
        timestamps = [float(row[0]) for row in rows]

        if not timestamps:
            return

        if isinstance(self.cadence.when, OnceAtEnd):
            yield timestamps[-1]
        else:
            last_run_at = None
            for i, timestamp_seconds in enumerate(timestamps):
                match self.cadence.when.unit:
                    case Unit.FRAME:
                        if i % self.cadence.when.every == 0:
                            yield timestamp_seconds
                    case _:
                        if (
                            last_run_at is None
                            or timestamp_seconds - last_run_at >= self.cadence.when.to_seconds()
                        ):
                            last_run_at = timestamp_seconds
                            yield timestamp_seconds

    def run_at(self, asof_seconds: float) -> None:
        """Run the pipeline at the given timestamp (in seconds)."""
        try:
            if all(gate.evaluate(asof_seconds, lookback) for gate, lookback in self._gates):
                for task, lookback in self._tasks:
                    task.execute(asof_seconds, lookback)
                logging.info(
                    "Pipeline '%s' executed successfully when topic '%s' received message at %.4f seconds",  # noqa: E501
                    self.name,
                    self.cadence.topic,
                    asof_seconds,
                )
            else:
                logging.info(
                    "Pipeline '%s' didn't pass the gating criteria when topic '%s' received message at %.4f seconds",  # noqa: E501
                    self.name,
                    self.cadence.topic,
                    asof_seconds,
                )
        except Exception as e:
            if not self._allow_failure:
                raise e
            logging.error(
                "Pipeline '%s' execution failed when topic '%s' received message at %.4f seconds: %s",  # noqa: E501
                self.name,
                self.cadence.topic,
                asof_seconds,
                str(e),
            )

    def run_all(self) -> None:
        """Run the pipeline at all applicable timestamps based on its cadence."""
        for asof_seconds in self._asof_timestamps():
            self.run_at(asof_seconds)
        logging.info("Pipeline '%s' completed.", self.name)

    @staticmethod
    def build(config: dict[str, Any]) -> "Pipeline":
        """Build a Pipeline instance from a YAML definition."""
        if "name" not in config:
            raise MissingRequiredKeyError("name")
        name = config["name"]

        if "path" not in config:
            raise MissingRequiredKeyError("path")
        path = config["path"]

        if "allow_failure" not in config:
            raise MissingRequiredKeyError("allow_failure")
        allow_failure = config["allow_failure"]

        if "cadence" not in config:
            raise MissingRequiredKeyError("cadence")
        cadence = Cadence.build(config["cadence"])

        gates = []
        for gate_config in config.get("gates", []):
            gate = Gate.build(path, gate_config)
            lookback = None
            if "lookback" in gate_config:
                lookback = Lookback.build(gate_config["lookback"])
            gates.append((gate, lookback))

        if "tasks" not in config:
            raise MissingRequiredKeyError("tasks")
        tasks = []
        for task_config in config["tasks"]:
            task = Task.build(path, task_config)
            lookback = None
            if "lookback" in task_config:
                lookback = Lookback.build(task_config["lookback"])
            tasks.append((task, lookback))

        return Pipeline(
            name=name,
            path=path,
            allow_failure=allow_failure,
            cadence=cadence,
            gates=gates,
            tasks=tasks,
        )
