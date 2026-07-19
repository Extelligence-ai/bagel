"""Flatten struct-shaped topic relations into plain scalar columns.

Bagel's standard relation shape is `timestamp_seconds` plus one STRUCT column per
topic -- great for SQL, but plotting tools (PlotJuggler's CSV/Parquet loaders, pandas,
spreadsheets) want one scalar column per signal. `flatten()` expands every struct
leaf into its own column named `topic/field/subfield`, matching the series naming
PlotJuggler users know from ROS (e.g. `/imu/linear_acceleration/x`).

Non-scalar leaves (lists, maps) are skipped -- they have no meaningful single-column
representation for plotting.
"""

import logging

import duckdb
import pyarrow as pa

from bagel.settings import settings
from bagel.source.postgres import quote_identifier

# Leaf types that become columns; anything else (lists, maps, unions) is skipped.
_SCALAR_CHECKS = (
    pa.types.is_integer,
    pa.types.is_floating,
    pa.types.is_boolean,
    pa.types.is_string,
    pa.types.is_large_string,
    pa.types.is_timestamp,
    pa.types.is_decimal,
)


def _is_scalar(dtype: pa.DataType) -> bool:
    return any(check(dtype) for check in _SCALAR_CHECKS)


def _leaf_selects(column: str, dtype: pa.DataType, path: list[str]) -> list[str]:
    """Build SELECT expressions for every scalar leaf under a (possibly nested) type."""
    if pa.types.is_struct(dtype):
        selects: list[str] = []
        for field in dtype:
            selects.extend(_leaf_selects(column, field.type, [*path, field.name]))
        return selects
    if not _is_scalar(dtype):
        logging.debug("Skipping non-scalar field '%s/%s'", column, "/".join(path))
        return []
    accessor = quote_identifier(column) + "".join(f"['{part}']" for part in path)
    alias = quote_identifier("/".join([column, *path]) if path else column)
    return [f"{accessor} AS {alias}"]


def flatten(relation: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Expand struct topic columns into one scalar column per signal.

    The `timestamp_seconds` column is kept first and unchanged; every struct column
    contributes `topic/field/subfield` columns for its scalar leaves.
    """
    schema = relation.limit(0).arrow().schema

    selects = []
    for field in schema:
        if field.name == settings.TIMESTAMP_SECONDS_COLUMN_NAME:
            selects.append(quote_identifier(field.name))
        else:
            selects.extend(_leaf_selects(field.name, field.type, []))
    return relation.project(", ".join(selects))
