"""Base class for PyArrow dataset source factories."""

import time
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import Any

import pandas as pd
from pyarrow import dataset as ds
from pydantic import BaseModel, ConfigDict

from src.source import base


class TimestampUnit(Enum):
    """Unit of the timestamp value."""

    SECONDS = "seconds"
    MILLISECONDS = "milliseconds"
    MICROSECONDS = "microseconds"
    NANOSECONDS = "nanoseconds"


class PyArrowDataset(BaseModel):
    """Represent a PyArrow dataset for files."""

    dataset: ds.Dataset
    extract_timestamp_seconds: Callable[[dict[str, Any]], float]

    model_config = ConfigDict(arbitrary_types_allowed=True)


_MISSING = object()


def _get_value(data: dict[str, Any], keys: list[str], default: object = _MISSING) -> object:
    """Return the value from a nested dictionary using the given keys."""
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, TypeError):
        if default is not _MISSING:
            return default
        raise


class SourceFactory(base.FileBasedSourceFactory):
    """Base class for factories of PyArrow dataset from local file system."""

    def __init__(  # noqa: PLR0913
        self,
        path: str,
        partitioning: str | list[str] | None = None,
        partition_base_dir: str | None = None,
        exclude_invalid_files: bool = True,
        ignore_prefixes: list[str] | None = None,
        timestamp_access_path: list[str] | None = None,
        timestamp_format: str | None = None,
    ) -> None:
        """Initialize the PyArrow dataset source factory."""
        super().__init__(path=path)

        # PyArrow Dataset arguments
        self._partitioning = partitioning
        self._partition_base_dir = partition_base_dir
        self._exclude_invalid_files = exclude_invalid_files
        self._ignore_prefixes = ignore_prefixes or []

        # Timestamp parsing
        self._timestamp_access_path = timestamp_access_path
        self._timestamp_format = timestamp_format

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the PyArrow dataset."""
        return {
            **self._file_based_metadata,
            "partitioning": self._partitioning,
            "partition_base_dir": self._partition_base_dir,
            "exclude_invalid_files": self._exclude_invalid_files,
            "ignore_prefixes": self._ignore_prefixes,
            "timestamp_access_path": self._timestamp_access_path,
            "timestamp_format": self._timestamp_format,
        }

    def _extract_timestamp_fn(self) -> Callable[[dict[str, Any]], float]:
        if self._timestamp_access_path is None:
            return lambda _: time.time()

        def cast_to_timestamp(value: object) -> float:
            if isinstance(value, pd.Timestamp):
                return value.timestamp()

            elif (
                isinstance(value, float) or isinstance(value, int)
            ) and self._timestamp_format in {unit.value for unit in TimestampUnit}:
                match TimestampUnit(self._timestamp_format):
                    case TimestampUnit.SECONDS:
                        return value
                    case TimestampUnit.MILLISECONDS:
                        return value / 1_000
                    case TimestampUnit.MICROSECONDS:
                        return value / 1_000_000
                    case TimestampUnit.NANOSECONDS:
                        return value / 1_000_000_000

            elif isinstance(value, str) and self._timestamp_format is not None:
                return datetime.strptime(value, self._timestamp_format).timestamp()

            else:
                raise ValueError(
                    f"Can't parse {value} to timestamp with format {self._timestamp_format}"
                )

        return lambda msg: cast_to_timestamp(_get_value(msg, self._timestamp_access_path))

    def _build(self, file_format: str) -> PyArrowDataset:
        return PyArrowDataset(
            dataset=ds.dataset(
                str(self.path),
                format=file_format,
                partitioning=self._partitioning,
                partition_base_dir=self._partition_base_dir,
                exclude_invalid_files=self._exclude_invalid_files,
                ignore_prefixes=self._ignore_prefixes,
            ),
            extract_timestamp_seconds=self._extract_timestamp_fn(),
        )
