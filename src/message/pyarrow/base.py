"""A message dataset for PyArrow dataset."""

from collections.abc import Iterator
from typing import Any

import duckdb
import pyarrow as pa

from src import query
from src.message import base
from src.source import errors
from src.source.pyarrow.base import PyArrowDataset
from src.topic.pyarrow.base import TOPIC_NAME


class MessageDataset(base.MessageDataset):
    """A message dataset for PyArrow dataset."""

    def _messages(
        self,
        data_source: PyArrowDataset,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, dict[str, Any]]]:
        if topics != [TOPIC_NAME]:
            raise ValueError(f"Only '{TOPIC_NAME}' topic is supported for PyArrow data source.")

        # dataset construction (_build(), src/source/pyarrow/base.py) can succeed
        # while still holding an unreadable file: e.g. a directory whose schema is
        # taken from one valid file, alongside a malformed sibling that only fails
        # once actually scanned. to_table() is where that scan -- and thus the raw
        # pyarrow.lib.ArrowInvalid/ArrowTypeError (ArrowException) -- happens.
        try:
            schema = pa.schema(
                [
                    ("ts", pa.float64()),
                    ("ordinal", pa.int64()),
                    ("message", pa.struct(data_source.dataset.schema)),
                ]
            )
            reader = pa.RecordBatchReader.from_batches(
                schema,
                self._timestamped_batches(
                    data_source, schema, start_seconds_inclusive, end_seconds_inclusive
                ),
            )
            # DuckDB sorts Arrow batches with disk spill rather than a Python heap
            # containing the complete decoded source. Ordinal preserves tied timestamps.
            ordered = query.from_arrow(reader).order("ts, ordinal")
            while rows := ordered.fetchmany(8192):
                for timestamp, _, message in rows:
                    yield (TOPIC_NAME, timestamp, message)
        except (pa.ArrowException, duckdb.Error) as exc:
            files = getattr(data_source.dataset, "files", None)
            location = ", ".join(files) if files else "the PyArrow dataset"
            raise errors.InvalidPathError(
                f"{location} could not be parsed: {type(exc).__name__}: {exc}"
            ) from exc

    def _timestamped_batches(
        self,
        data_source: PyArrowDataset,
        schema: pa.Schema,
        start_seconds: float | None,
        end_seconds: float | None,
    ) -> Iterator[pa.RecordBatch]:
        ordinal = 0
        for batch in data_source.dataset.to_batches(batch_size=8192):
            records = []
            for msg in batch.to_pylist():
                timestamp = data_source.extract_timestamp_seconds(msg)
                ordinal += 1
                if start_seconds is not None and timestamp < start_seconds:
                    continue
                if end_seconds is not None and timestamp > end_seconds:
                    continue
                records.append({"ts": timestamp, "ordinal": ordinal, "message": msg})
            if records:
                yield pa.RecordBatch.from_pylist(records, schema=schema)

    def _to_json(self, message: object, struct: pa.StructType) -> dict[str, Any]:
        return message
