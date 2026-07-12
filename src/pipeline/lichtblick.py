"""Export an event window as a Lichtblick session: MCAP data + a pre-framed layout.

Writes the window as an MCAP file with JSON-encoded channels (schema encoding
``jsonschema``), which Lichtblick -- BMW's open-source fork of Foxglove Studio --
reads natively, plus a layout ``.json`` (schema matching real Lichtblick layout
fixtures) with a Plot panel whose series and x/y ranges are pre-set to the event.
The same files open in Foxglove, which shares the layout format.

Signal names follow the flattened ``topic/field/subfield`` convention used by the
PlotJuggler and Rerun exports; they are translated to Lichtblick message paths
(``/imu.linear_acceleration.x``) in the layout.
"""

import json
import logging
import pathlib
import re
from typing import Any

import duckdb
import pyarrow as pa
from mcap.writer import Writer

from settings import settings
from src.pipeline import flatten as flatten_module
from src.pipeline.plotjuggler import MAX_CURVES, _numeric_columns
from src.source.postgres import quote_identifier

SECOND_NS = 1_000_000_000


def _jsonschema_type(dtype: pa.DataType) -> dict[str, Any]:
    """Map an Arrow type to a JSON-Schema fragment (reverse of the MCAP reader's mapper)."""
    if pa.types.is_struct(dtype):
        return {
            "type": "object",
            "properties": {field.name: _jsonschema_type(field.type) for field in dtype},
        }
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return {"type": "array", "items": _jsonschema_type(dtype.value_type)}
    if pa.types.is_integer(dtype):
        return {"type": "integer"}
    if pa.types.is_floating(dtype) or pa.types.is_decimal(dtype):
        return {"type": "number"}
    if pa.types.is_boolean(dtype):
        return {"type": "boolean"}
    return {"type": "string"}


def _message_paths(schema: pa.Schema) -> dict[str, str]:
    """Map every flattened scalar signal name to its Lichtblick message path."""
    paths: dict[str, str] = {}

    def walk(topic: str, dtype: pa.DataType, parts: list[str]) -> None:
        if pa.types.is_struct(dtype):
            for field in dtype:
                walk(topic, field.type, [*parts, field.name])
        elif parts:
            paths["/".join([topic, *parts])] = topic + "." + ".".join(parts)

    for field in schema:
        if field.name != settings.TIMESTAMP_SECONDS_COLUMN_NAME:
            walk(field.name, field.type, [])
    return paths


def write_mcap(relation: duckdb.DuckDBPyRelation, mcap_file: pathlib.Path) -> None:
    """Write the relation's topics as JSON-encoded MCAP channels."""
    table = relation.arrow()
    topics = [
        field.name for field in table.schema if field.name != settings.TIMESTAMP_SECONDS_COLUMN_NAME
    ]
    with open(mcap_file, "wb") as stream:
        writer = Writer(stream)
        writer.start(profile="", library="bagel")
        channels = {}
        for topic in topics:
            schema_id = writer.register_schema(
                name=topic,
                encoding="jsonschema",
                data=json.dumps(_jsonschema_type(table.schema.field(topic).type)).encode(),
            )
            channels[topic] = writer.register_channel(
                topic=topic, message_encoding="json", schema_id=schema_id
            )
        for row in table.to_pylist():
            log_time = int(row[settings.TIMESTAMP_SECONDS_COLUMN_NAME] * SECOND_NS)
            for topic in topics:
                if row[topic] is None:
                    continue
                writer.add_message(
                    channel_id=channels[topic],
                    log_time=log_time,
                    publish_time=log_time,
                    data=json.dumps(row[topic]).encode(),
                )
        writer.finish()


def build_layout(
    paths: list[str],
    x_range: tuple[float, float],
    y_range: tuple[float, float],
    tab_title: str,
) -> str:
    """Build a Lichtblick layout JSON string with one pre-framed Plot panel."""
    plot_id = "Plot!bagel"
    layout = {
        "configById": {
            plot_id: {
                "title": tab_title,
                "paths": [
                    {"value": path, "enabled": True, "timestampMethod": "receiveTime"}
                    for path in paths
                ],
                "showXAxisLabels": True,
                "showYAxisLabels": True,
                "showLegend": True,
                "legendDisplay": "floating",
                "showPlotValuesInLegend": False,
                "isSynced": True,
                "xAxisVal": "timestamp",
                "minXValue": x_range[0],
                "maxXValue": x_range[1],
                "minYValue": y_range[0],
                "maxYValue": y_range[1],
                "sidebarDimension": 240,
            }
        },
        "globalVariables": {},
        "userNodes": {},
        "playbackConfig": {"speed": 1},
        "layout": plot_id,
    }
    return json.dumps(layout, indent=2)


def export_window(  # noqa: PLR0913
    relation: duckdb.DuckDBPyRelation,
    name: str,
    start_seconds: float,
    end_seconds: float,
    signals: list[str] | None = None,
    output_directory: str | pathlib.Path | None = None,
) -> dict[str, Any]:
    """Write an MCAP of the relation plus a Lichtblick layout framing it.

    Args:
        relation: A standard Bagel relation (timestamp_seconds + struct topic columns),
            already restricted to the window of interest.
        name: Session name (used for the output directory and plot title).
        start_seconds: Window start (frames the plot's x range).
        end_seconds: Window end.
        signals: Flattened signal names to plot (e.g. "/imu/linear_acceleration/x").
            If None, all numeric signals are plotted, capped at MAX_CURVES.
        output_directory: Where to write. Defaults to
            `<ARTIFACT_DIRECTORY>/lichtblick/<name>`.

    Returns:
        ``{"mcap", "layout", "curves", "instructions"}`` -- the written paths, plotted
        message paths, and how to open the session.

    Raises:
        ValueError: If a requested signal does not exist or nothing is plottable.

    """
    flat = flatten_module.flatten(relation)

    available = _numeric_columns(flat)
    if signals is not None:
        unusable = sorted(set(signals) - set(available))
        if unusable:
            raise ValueError(
                f"Unknown or non-numeric signals: {unusable}. "
                f"Available numeric signals: {available}"
            )
        curves = list(signals)
    else:
        curves = available[:MAX_CURVES]
        if len(available) > MAX_CURVES:
            logging.info(
                "Plotting the first %d of %d numeric signals; pass 'signals' to choose",
                MAX_CURVES,
                len(available),
            )
    if not curves:
        raise ValueError("No numeric signals to plot in the selected topics.")

    name = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "event"
    directory = (
        pathlib.Path(output_directory)
        if output_directory
        else pathlib.Path(settings.ARTIFACT_DIRECTORY) / "lichtblick" / name
    )
    directory.mkdir(parents=True, exist_ok=True)
    mcap_file = directory / f"{name}.mcap"
    layout_file = directory / f"{name}_layout.json"

    write_mcap(relation, mcap_file)

    # Frame the y range from the actual data (10% padding), like the PlotJuggler export.
    aggregates = ", ".join(
        f"MIN({quote_identifier(c)}), MAX({quote_identifier(c)})" for c in curves
    )
    bounds = flat.aggregate(aggregates).fetchone()
    minima = [b for b in bounds[0::2] if b is not None]
    maxima = [b for b in bounds[1::2] if b is not None]
    low = float(min(minima)) if minima else 0.0
    high = float(max(maxima)) if maxima else 1.0
    pad = (high - low) * 0.1 or 1.0

    message_paths = _message_paths(relation.limit(0).arrow().schema)
    paths = [message_paths[curve] for curve in curves]
    layout_file.write_text(
        build_layout(
            paths,
            x_range=(start_seconds, end_seconds),
            y_range=(low - pad, high + pad),
            tab_title=name,
        )
    )

    return {
        "mcap": str(mcap_file),
        "layout": str(layout_file),
        "curves": paths,
        "instructions": (
            f"Open Lichtblick (or Foxglove), drop in {mcap_file}, then "
            f"Layouts -> Import from file -> {layout_file}."
        ),
    }
