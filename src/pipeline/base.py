"""Base classes and utilities for defining and running data processing pipelines."""

import abc
import importlib
import logging
import pathlib
from collections.abc import Iterator
from enum import Enum
from typing import Any

import boto3
import botocore
from pydantic import BaseModel

from settings import settings
from src import artifacts
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import resolve
from src.pipeline import progress

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

    # Attributes set during Pipeline.build
    _name: str
    _pipeline: str
    _site: str
    _asset: str
    _path: str
    _log_id: str
    _upload: bool

    @property
    def name(self) -> str:
        """The name of the operator."""
        return self._name

    @property
    def pipeline(self) -> str:
        """The name of the parent pipeline."""
        return self._pipeline

    @property
    def site(self) -> str:
        """The name of the deployment site."""
        return self._site

    @property
    def asset(self) -> str:
        """The name of the asset in the deployment site."""
        return self._asset

    @property
    def path(self) -> str:
        """The filesystem path or URL to the data source."""
        return self._path

    @property
    def log_id(self) -> str:
        """A unique identifier for the path of an asset in a deployment site."""
        return self._log_id

    @property
    def upload(self) -> bool:
        """Whether to upload artifacts to remote storage."""
        return self._upload

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
    def build(
        pipeline: str,
        site: str,
        asset: str,
        path: str,
        config: dict[str, Any],
    ) -> "Operator":
        """Build an Operator instance from a config dictionary of a gate or task.

        Args:
            pipeline (str): The name of the pipeline.
            site (str): The name of the deployment site.
            asset (str): The name of the asset in the deployment site.
            path (str): Filesystem path or URL to the data source.
            config (dict[str, Any]): Configuration dictionary for the operator.

        Returns:
            Operator: The constructed Operator instance.

        """
        importlib.import_module(config["module"]).register()
        cls = module.global_registry[config["module"]]
        instance: Operator = cls(**config.get("args", {}))
        instance.setup(path=path, **config.get("setup", {}))

        name = config.get("name", artifacts.to_lower_snake_case(cls.__name__))
        if not artifacts.is_lower_snake_case(name):
            raise ValueError(f"Operator name '{name}' is not in lower_snake_case format.")
        instance._name = name
        instance._pipeline = pipeline
        instance._site = site
        instance._asset = asset
        instance._path = path
        instance._log_id = artifacts.generate_log_uuid(site, asset, path)

        instance._upload = config.get("upload", False)
        # TODO: support upload for gates
        if isinstance(instance, Gate) and instance.upload:
            logging.warning(
                "Upload option for gates is currently not supported and will be ignored."
            )

        return instance


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
    def execute(self, asof_seconds: float, lookback: Lookback | None) -> list[pathlib.Path] | None:
        """Execute the task at a specific point in time.

        Args:
            asof_seconds (float): The timestamp (in seconds) at which to execute the task.
            lookback (Lookback | None): The lookback window defining how far back to consider data.
                If None, all available data up to `asof_seconds` should be considered. Some tasks
                may not support lookback and will ignore this argument.

        Returns:
            list[pathlib.Path] | None: The paths to any generated artifacts. If None, the task does
                not produce any artifacts. An artifact could be a file or directory path.

        """


class Pipeline:
    """A data processing pipeline consisting of gates and tasks.

    It runs at specified intervals based on a cadence, evaluates gates,
    and executes tasks if the gates pass.

    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        site: str,
        asset: str,
        path: str,
        allow_failure: bool,
        cadence: Cadence,
        gates: list[tuple[Gate, Lookback | None]],
        tasks: list[tuple[Task, Lookback | None]],
        report_progress: bool,
    ) -> None:
        """Initialize a Pipeline instance.

        Args:
            name (str): The name of the pipeline, in lower_snake_case format.
            site (str): The name of the deployment site, in lower_snake_case format.
            asset (str): The name of the asset in the deployment site, in lower_snake_case format.
            path (str): Filesystem path or URL to the data source.
            allow_failure (bool): Whether to continue executing pipeline runs if a run fails.
            cadence (Cadence): How often to run the pipeline.
            gates (list[tuple[Gate, Lookback  |  None]]): List of gating operators and their
                lookback windows.
            tasks (list[tuple[Task, Lookback  |  None]]): List of task operators and their
                lookback windows.
            report_progress (bool): Whether to report progress during artifact uploads.

        """
        if not artifacts.is_lower_snake_case(name):
            raise ValueError(f"Pipeline name '{name}' is not in lower_snake_case format.")
        self._name = name

        if not artifacts.is_lower_snake_case(site):
            raise ValueError(f"Site name '{site}' is not in lower_snake_case format.")
        self._site = site

        if not artifacts.is_lower_snake_case(asset):
            raise ValueError(f"Asset name '{asset}' is not in lower_snake_case format.")
        self._asset = asset

        self._path = path
        self._allow_failure = allow_failure
        self._cadence = cadence
        self._gates = gates
        self._tasks = tasks

        self._report_progress = report_progress
        self._artifacts = []

    @property
    def name(self) -> str:
        """The name of the pipeline."""
        return self._name

    @property
    def site(self) -> str:
        """The name of the deployment site."""
        return self._site

    @property
    def asset(self) -> str:
        """The name of the asset in the deployment site."""
        return self._asset

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

        logging.info("Gathering timestamps for pipeline '%s'...", self.name)
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
                    artifacts = task.execute(asof_seconds, lookback)
                    if task.upload and artifacts and self.can_upload_artifacts():
                        self._artifacts.extend(artifacts)
                logging.info(
                    "Pipeline '%s' executed when topic '%s' received message at %.4f seconds",
                    self.name,
                    self.cadence.topic,
                    asof_seconds,
                )
            else:
                logging.info(
                    "Pipeline '%s' skipped when topic '%s' received message at %.4f seconds",
                    self.name,
                    self.cadence.topic,
                    asof_seconds,
                )
        except Exception as e:
            if not self._allow_failure:
                raise e
            logging.error(
                "Pipeline '%s' failed when topic '%s' received message at %.4f seconds: %s",
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

        if self._artifacts:
            self.upload_artifacts()
            logging.info("Uploaded artifacts for pipeline '%s'.", self.name)

    @staticmethod
    def build(config: dict[str, Any]) -> "Pipeline":
        """Build a Pipeline instance from a YAML definition."""
        required_keys = ["name", "site", "asset", "path", "allow_failure", "cadence", "tasks"]
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise MissingRequiredKeyError(", ".join(missing_keys))

        pipeline = config["name"]
        site = config["site"]
        asset = config["asset"]
        path = config["path"]

        gates = []
        for gate_config in config.get("gates", []):
            gate = Gate.build(pipeline, site, asset, path, gate_config)
            lookback = None
            if "lookback" in gate_config:
                lookback = Lookback.build(gate_config["lookback"])
            gates.append((gate, lookback))

        if "tasks" not in config:
            raise MissingRequiredKeyError("tasks")
        tasks = []
        for task_config in config["tasks"]:
            task = Task.build(pipeline, site, asset, path, task_config)
            lookback = None
            if "lookback" in task_config:
                lookback = Lookback.build(task_config["lookback"])
            tasks.append((task, lookback))

        return Pipeline(
            name=pipeline,
            site=site,
            asset=asset,
            path=path,
            allow_failure=config["allow_failure"],
            cadence=Cadence.build(config["cadence"]),
            gates=gates,
            tasks=tasks,
            report_progress=config.get("report_progress", False),
        )

    def can_upload_artifacts(self) -> bool:
        """Check if the pipeline is configured to upload artifacts."""
        if not settings.EXTELLIGENCE_S3_BUCKET_NAME:
            logging.warning(
                "Cannot upload artifacts for pipeline '%s' because 'EXTELLIGENCE_S3_BUCKET_NAME' is not set",  # noqa: E501
                self.name,
            )
            return False

        return True

    def upload_artifacts(self) -> None:
        """Upload generated artifacts to remote storage."""
        for path in self._artifacts:
            self._upload(path)
        self._artifacts = []

    def _upload(self, path: pathlib.Path) -> None:
        if not path.exists():
            raise FileNotFoundError(path)

        source_files = []

        if path.is_file():
            source_files.append(path)
        elif path.is_dir():
            source_files.extend([p for p in path.rglob("*") if p.is_file()])

        s3_client = boto3.client("s3", region_name=settings.EXTELLIGENCE_S3_BUCKET_REGION)

        for local_file in source_files:
            local_sha256 = artifacts.checksum_sha256(local_file)
            s3_key = artifacts.artifact_s3_key(local_file)

            try:
                response = s3_client.get_object_attributes(
                    Bucket=settings.EXTELLIGENCE_S3_BUCKET_NAME,
                    Key=s3_key,
                    ObjectAttributes=["Checksum"],
                )
                checksum = response.get("Checksum", {})
                remote_sha256 = checksum.get("ChecksumSHA256")
                if remote_sha256 == local_sha256:
                    logging.info(
                        "'%s' already exists in S3 with matching SHA-256, skipping upload", s3_key
                    )
                    continue
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    pass  # Object does not exist, proceed to upload
                else:
                    raise e

            bar = progress.Progress(
                local_file,
                s3_key,
                local_file.stat().st_size,
                "local file",
                "s3 file",
                self._report_progress,
            )

            s3_client.upload_file(
                Filename=str(local_file),
                Bucket=settings.EXTELLIGENCE_S3_BUCKET_NAME,
                Key=s3_key,
                Callback=bar,
                ExtraArgs={"ChecksumAlgorithm": "SHA256"},
            )
