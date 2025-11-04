"""Provide a PyArrow dataset for reading JSON files."""

import logging
from collections.abc import Callable
from enum import Enum
from typing import Any

from pyarrow import dataset as ds
from pydantic import BaseModel, ConfigDict

from src.di import module
from src.di.types.data_source import is_json_directory, is_json_file
from src.source import base


class TimestampUnit(Enum):
    """Unit of the timestamp value."""

    SECONDS = "seconds"
    MILLISECONDS = "milliseconds"
    MICROSECONDS = "microseconds"
    NANOSECONDS = "nanoseconds"


class PyArrowDataset(BaseModel):
    """Represent a PyArrow dataset for JSON files."""

    dataset: ds.Dataset
    extract_timestamp_seconds: Callable[[dict[str, Any]], float]

    model_config = ConfigDict(arbitrary_types_allowed=True)


_MISSING = object()


def get_value(data: dict[str, Any], keys: list[str], default: object = _MISSING) -> object:
    """Return the value from a nested dictionary using the given keys."""
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, TypeError):
        if default is not _MISSING:
            return default
        raise


def to_seconds_from_unit(msg: dict[str, Any], access_path: list[str], unit: TimestampUnit) -> float:
    """Cast the timestamp value to seconds based on the given unit."""
    value = get_value(msg, access_path)
    match unit:
        case TimestampUnit.SECONDS:
            return float(value)
        case TimestampUnit.MILLISECONDS:
            return float(value) / 1_000
        case TimestampUnit.MICROSECONDS:
            return float(value) / 1_000_000
        case TimestampUnit.NANOSECONDS:
            return float(value) / 1_000_000_000
        case _:
            raise ValueError(f"Unsupported timestamp unit: {unit}")


def to_seconds_from_format(msg: dict[str, Any], access_path: list[str], format_str: str) -> float:
    """Parse the timestamp value to seconds from the given format string."""
    from datetime import datetime

    value = get_value(msg, access_path)
    return datetime.strptime(value, format_str).timestamp()


class SourceFactory(base.FileBasedSourceFactory):
    """A data source factory for reading json files as a PyArrow dataset."""

    def __init__(  # noqa: PLR0913
        self,
        path: str,
        partitioning: str | list[str] | None = None,
        partition_base_dir: str | None = None,
        exclude_invalid_files: bool = True,
        ignore_prefixes: list[str] | None = None,
        timestamp_access_path: list[str] | None = None,
        timestamp_unit: str | None = None,
        timestamp_format: str | None = None,
    ) -> None:
        """Initialize a PyArrow JSON data source factory.

        Many of the arguments are directly passed to pyarrow.dataset.dataset().
        https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html

        Args:
            path (str): Path to the JSON file or directory.
            partitioning (str | list[str] | None, optional): The partitioning scheme specified with
                the partitioning() function. A flavor string can be used as shortcut, and with a
                list of field names a DirectoryPartitioning will be inferred.
            partition_base_dir (str | None, optional): For the purposes of applying the
                partitioning, paths will be stripped of the partition_base_dir. Files not matching
                the partition_base_dir prefix will be skipped for partitioning discovery.
                The ignored files will still be part of the Dataset, but will not have partition
                information.
            exclude_invalid_files (bool, optional): If True, invalid files will be excluded
                (file format specific check). This will incur IO for each files in a serial and
                single threaded fashion. Disabling this feature will skip the IO, but unsupported
                files may be present in the Dataset (resulting in an error at scan time).
            ignore_prefixes (list[str] | None, optional): Files matching any of these prefixes will
                be ignored by the discovery process. This is matched to the basename of a path.
                By default this is ['.', '_']. Note that discovery happens only if a directory is
                passed as source.
            timestamp_access_path (list[str] | None, optional): A list of access keys to extract
                the timestamp value from each message. If None, the current system time is used.
            timestamp_unit (str | None, optional): The unit of the timestamp value. Must be one of
                'seconds', 'milliseconds', 'microseconds', or 'nanoseconds'.
            timestamp_format (str | None, optional): The format string to parse the timestamp value.
                This is only used if 'timestamp_access_path' is provided and 'timestamp_unit'
                is None.

        Raises:
            ValueError: If 'timestamp_access_path' is provided but neither 'timestamp_unit' nor
                'timestamp_format' is provided.

        """
        super().__init__(path=path)

        self._partitioning = partitioning
        self._partition_base_dir = partition_base_dir
        self._exclude_invalid_files = exclude_invalid_files
        self._ignore_prefixes = ignore_prefixes or []

        self._timestamp_access_path = timestamp_access_path
        self._timestamp_unit = TimestampUnit(timestamp_unit) if timestamp_unit else None
        self._timestamp_format = timestamp_format

        match self._timestamp_access_path, self._timestamp_unit, self._timestamp_format:
            case None, None, None:
                pass

            case _, None, None:
                raise ValueError(
                    "If 'timestamp_access_path' is provided, "
                    "either 'timestamp_unit' or 'timestamp_format' must also be provided."
                )

            case None, _, _ if self._timestamp_unit or self._timestamp_format:
                logging.debug(
                    "'timestamp_access_path' is not provided, "
                    "ignoring 'timestamp_unit' and 'timestamp_format'."
                )

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the topic sink."""
        return {
            **self._file_based_metadata,
            "partitioning": self._partitioning,
            "partition_base_dir": self._partition_base_dir,
            "exclude_invalid_files": self._exclude_invalid_files,
            "ignore_prefixes": self._ignore_prefixes,
            "timestamp_access_path": self._timestamp_access_path,
            "timestamp_unit": self._timestamp_unit.value if self._timestamp_unit else None,
            "timestamp_format": self._timestamp_format,
        }

    def _extract_timestamp_fn(self) -> Callable[[dict[str, Any]], float]:
        match self._timestamp_access_path, self._timestamp_unit, self._timestamp_format:
            case None, _, _:
                import time

                return lambda _: time.time()

            case access_path, unit, None:
                return lambda msg: to_seconds_from_unit(msg, access_path, unit)

            case access_path, None, format_str:
                return lambda msg: to_seconds_from_format(msg, access_path, format_str)

    def build(self) -> PyArrowDataset:
        """Return a PyArrowDataset object."""
        return PyArrowDataset(
            dataset=ds.dataset(
                str(self.path),
                format="json",
                partitioning=self._partitioning,
                partition_base_dir=self._partition_base_dir,
                exclude_invalid_files=self._exclude_invalid_files,
                ignore_prefixes=self._ignore_prefixes,
            ),
            extract_timestamp_seconds=self._extract_timestamp_fn(),
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate if the given path is a valid JSON or JSONL file/directory."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not is_json_file(self.path) and not is_json_directory(self.path):
            return False, ValueError(f"{self.path} is not a valid JSON or JSONL file/directory.")

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
