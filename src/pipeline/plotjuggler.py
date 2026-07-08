"""Export an event window as a PlotJuggler session: flattened CSV + layout file.

Generates a layout ``.xml`` (schema matching real PlotJuggler >= 3.x layouts, e.g. the
PX4 sample views) that references the exported CSV, pre-adds the signal curves, and
frames the time/value ranges -- so ``plotjuggler -l <layout>`` opens the event already
plotted and zoomed. Curve names are the flattened column headers
(``/imu/linear_acceleration/x``), which is exactly how PlotJuggler's CSV loader names
series.
"""

import logging
import pathlib
import re
import xml.etree.ElementTree as ET
from typing import Any

import duckdb

from settings import settings
from src.pipeline import flatten as flatten_module
from src.source.postgres import quote_identifier

# PlotJuggler's default curve palette (as seen in its saved layouts).
PALETTE = ("#1f77b4", "#d62728", "#1ac938", "#ff7f0e", "#f14cc1", "#9467bd", "#17becf")

MAX_CURVES = 8

_NUMERIC_PREFIXES = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT",
                     "USMALLINT", "UINTEGER", "UBIGINT", "FLOAT", "DOUBLE", "DECIMAL")


def _numeric_columns(relation: duckdb.DuckDBPyRelation) -> list[str]:
    """Return the plottable (numeric) columns, excluding the timestamp."""
    return [
        name
        for name, dtype in zip(relation.columns, relation.types, strict=True)
        if name != settings.TIMESTAMP_SECONDS_COLUMN_NAME
        and str(dtype).upper().startswith(_NUMERIC_PREFIXES)
    ]


def build_layout(  # noqa: PLR0913
    csv_file: pathlib.Path,
    curves: list[str],
    x_range: tuple[float, float],
    y_range: tuple[float, float],
    time_axis: str,
    tab_name: str,
) -> str:
    """Build a PlotJuggler layout XML string with one pre-framed time-series plot."""
    root = ET.Element("root")

    tabbed = ET.SubElement(root, "tabbed_widget", parent="main_window", name="Main Window")
    tab = ET.SubElement(tabbed, "Tab", containers="1", tab_name=tab_name)
    container = ET.SubElement(tab, "Container")
    splitter = ET.SubElement(container, "DockSplitter", count="1", orientation="-", sizes="1")
    dock = ET.SubElement(splitter, "DockArea", name="...")
    plot = ET.SubElement(
        dock, "plot", style="Lines", mode="TimeSeries", flip_y="false", flip_x="false"
    )
    ET.SubElement(
        plot,
        "range",
        bottom=f"{y_range[0]:.6f}",
        top=f"{y_range[1]:.6f}",
        left=f"{x_range[0]:.6f}",
        right=f"{x_range[1]:.6f}",
    )
    ET.SubElement(plot, "limitY")
    for i, curve in enumerate(curves):
        ET.SubElement(plot, "curve", color=PALETTE[i % len(PALETTE)], name=curve)
    ET.SubElement(tabbed, "currentTabIndex", index="0")

    # Absolute timestamps so the framed range matches the data's epoch seconds.
    ET.SubElement(root, "use_relative_time_offset", enabled="0")

    plugins = ET.SubElement(root, "Plugins")
    csv_plugin = ET.SubElement(plugins, "plugin", ID="DataLoad CSV")
    ET.SubElement(csv_plugin, "default", delimiter="0", time_axis=time_axis)

    datafiles = ET.SubElement(root, "previouslyLoaded_Datafiles")
    file_info = ET.SubElement(datafiles, "fileInfo", prefix="", filename=str(csv_file))
    ET.SubElement(file_info, "selected_datasources", value=";".join(curves))
    ET.SubElement(file_info, "plugin", ID="DataLoad CSV")

    ET.SubElement(root, "customMathEquations")
    ET.SubElement(root, "snippets")

    ET.indent(root, space=" ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def export_window(  # noqa: PLR0913
    relation: duckdb.DuckDBPyRelation,
    name: str,
    start_seconds: float,
    end_seconds: float,
    signals: list[str] | None = None,
    output_directory: str | pathlib.Path | None = None,
) -> dict[str, Any]:
    """Write a flattened CSV of the relation plus a PlotJuggler layout framing it.

    Args:
        relation: A standard Bagel relation (timestamp_seconds + struct topic columns),
            already restricted to the window of interest.
        name: Session name (used for the output directory and tab title).
        start_seconds: Window start (frames the plot's x range).
        end_seconds: Window end.
        signals: Flattened signal names to plot (e.g. "/imu/linear_acceleration/x").
            If None, all numeric signals are plotted, capped at MAX_CURVES.
        output_directory: Where to write. Defaults to
            `<ARTIFACT_DIRECTORY>/plotjuggler/<name>`.

    Returns:
        ``{"csv", "layout", "curves", "command"}`` -- the written paths, plotted
        curves, and the command that opens the session pre-framed.

    Raises:
        ValueError: If a requested signal does not exist or nothing is plottable.

    """
    flat = flatten_module.flatten(relation)

    available = _numeric_columns(flat)
    if signals is not None:
        missing = sorted(set(signals) - set(flat.columns))
        if missing:
            raise ValueError(
                f"Unknown signals: {missing}. Available numeric signals: {available}"
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
        else pathlib.Path(settings.ARTIFACT_DIRECTORY) / "plotjuggler" / name
    )
    directory.mkdir(parents=True, exist_ok=True)
    csv_file = directory / f"{name}.csv"
    layout_file = directory / f"{name}_layout.xml"

    flat.to_csv(file_name=str(csv_file))

    # Frame the y range from the actual data (10% padding).
    aggregates = ", ".join(
        f"MIN({quote_identifier(c)}), MAX({quote_identifier(c)})" for c in curves
    )
    bounds = flat.aggregate(aggregates).fetchone()
    minima = [b for b in bounds[0::2] if b is not None]
    maxima = [b for b in bounds[1::2] if b is not None]
    low = float(min(minima)) if minima else 0.0
    high = float(max(maxima)) if maxima else 1.0
    pad = (high - low) * 0.1 or 1.0
    y_range = (low - pad, high + pad)

    layout_file.write_text(
        build_layout(
            csv_file=csv_file,
            curves=curves,
            x_range=(start_seconds, end_seconds),
            y_range=y_range,
            time_axis=settings.TIMESTAMP_SECONDS_COLUMN_NAME,
            tab_name=name,
        )
    )
    logging.info("Wrote %s and %s", csv_file, layout_file)

    return {
        "csv": str(csv_file),
        "layout": str(layout_file),
        "curves": curves,
        "command": f"plotjuggler -n -l '{layout_file}'",
    }
