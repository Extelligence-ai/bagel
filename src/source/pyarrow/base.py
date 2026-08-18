"""Base class for PyArrow dataset source factories."""

import logging
import pathlib
import time
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import Any

import pandas as pd
import pyarrow as pa
from pyarrow import dataset as ds
from pydantic import BaseModel, ConfigDict

from src.source import base, errors


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

        # Populated by _build(): files PyArrow silently excluded as invalid (#134).
        self._excluded_file_count = 0

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
            "excluded_file_count": self._excluded_file_count,
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
        # ds.dataset() is lazy about reading row *data*, but for CSV/JSON it must
        # still infer a schema immediately, which requires reading (part of) the
        # file. When exclude_invalid_files=True (the default) PyArrow silently
        # drops any file that fails this check; but that check is an explicit,
        # user-settable constructor argument (see csv.SourceFactory /
        # json.SourceFactory docstrings), and with it disabled a malformed file
        # raises PyArrow's internal parse error -- pyarrow.lib.ArrowInvalid (or a
        # sibling ArrowException such as ArrowTypeError) -- directly out of this
        # call. Translate that into a single clean, typed error instead of
        # letting a raw PyArrow-internal traceback escape.
        try:
            dataset = ds.dataset(
                str(self.path),
                format=file_format,
                partitioning=self._partitioning,
                partition_base_dir=self._partition_base_dir,
                exclude_invalid_files=self._exclude_invalid_files,
                ignore_prefixes=self._ignore_prefixes,
            )
        except pa.ArrowException as exc:
            raise errors.InvalidPathError(
                f"{self.path} could not be parsed as a {file_format} dataset: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        # exclude_invalid_files=True silently drops files that fail the format
        # check. Silent-empty is indistinguishable from "no events found" to a
        # caller, so: zero readable files is an error; a partial drop keeps the
        # good files but is logged and counted in metadata (#134).
        discovered = getattr(dataset, "files", None)
        if self._exclude_invalid_files and discovered is not None:
            if not discovered:
                raise errors.InvalidPathError(
                    f"{self.path} contains no readable {file_format} files: "
                    "every candidate file was excluded as invalid"
                )
            self._excluded_file_count = max(0, self._candidate_file_count() - len(discovered))
            if self._excluded_file_count:
                logging.warning(
                    "%d file(s) under %s were excluded as invalid %s and are absent "
                    "from query results (see excluded_file_count in source metadata)",
                    self._excluded_file_count,
                    self.path,
                    file_format,
                )

        return PyArrowDataset(
            dataset=dataset,
            extract_timestamp_seconds=self._extract_timestamp_fn(),
        )

    def _candidate_file_count(self) -> int:
        """Count files the dataset discovery would have considered."""
        if self.path.is_file():
            return 1
        ignored = tuple(self._ignore_prefixes)

        def _is_ignored(file: pathlib.Path) -> bool:
            parts = file.relative_to(self.path).parts
            return bool(ignored) and any(part.startswith(ignored) for part in parts)

        return sum(1 for file in self.path.rglob("*") if file.is_file() and not _is_ignored(file))
