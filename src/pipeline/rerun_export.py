"""Export an event window as a Rerun recording (.rrd).

Writes every scalar signal in the window as a Rerun time series, so
``rerun <file>.rrd`` opens the event in the Rerun viewer with the full signal tree
ready to explore. Entity paths are the flattened signal names
(``/imu/linear_acceleration/x``), matching the naming used by the PlotJuggler export.

Requires the optional ``rerun-sdk`` dependency: ``uv sync --group viz``.
"""

import pathlib
import re
from typing import Any

import duckdb

from settings import settings
from src.pipeline import flatten as flatten_module
from src.pipeline.plotjuggler import _numeric_columns


def export_window(  # noqa: PLR0913
    relation: duckdb.DuckDBPyRelation,
    name: str,
    start_seconds: float,
    end_seconds: float,
    signals: list[str] | None = None,
    output_directory: str | pathlib.Path | None = None,
) -> dict[str, Any]:
    """Write a Rerun recording of the relation's scalar signals.

    Args:
        relation: A standard Bagel relation (timestamp_seconds + struct topic columns),
            already restricted to the window of interest.
        name: Recording name (used for the output directory and file).
        start_seconds: Window start, echoed into the result for context.
        end_seconds: Window end.
        signals: Flattened signal names to include (e.g. "/imu/linear_acceleration/x").
            If None, all numeric signals are included.
        output_directory: Where to write. Defaults to
            `<ARTIFACT_DIRECTORY>/rerun/<name>`.

    Returns:
        ``{"rrd", "signals", "command"}`` -- the written recording, included signals,
        and the command that opens it in the Rerun viewer.

    Raises:
        ImportError: If the optional rerun-sdk dependency is not installed.
        ValueError: If a requested signal does not exist or nothing is plottable.

    """
    try:
        import rerun as rr
    except ImportError as error:  # pragma: no cover -- exercised only without the dep
        raise ImportError(
            "The Rerun export needs the optional 'rerun-sdk' dependency; "
            "install it with: uv sync --group viz"
        ) from error

    flat = flatten_module.flatten(relation)

    available = _numeric_columns(flat)
    if signals is not None:
        unusable = sorted(set(signals) - set(available))
        if unusable:
            raise ValueError(
                f"Unknown or non-numeric signals: {unusable}. "
                f"Available numeric signals: {available}"
            )
        included = list(signals)
    else:
        included = available
    if not included:
        raise ValueError("No numeric signals to export in the selected topics.")

    name = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "event"
    directory = (
        pathlib.Path(output_directory)
        if output_directory
        else pathlib.Path(settings.ARTIFACT_DIRECTORY) / "rerun" / name
    )
    directory.mkdir(parents=True, exist_ok=True)
    rrd_file = directory / f"{name}.rrd"

    columns = flat.select(
        ", ".join(f'"{c}"' for c in [settings.TIMESTAMP_SECONDS_COLUMN_NAME, *included])
    ).fetchnumpy()
    times = columns[settings.TIMESTAMP_SECONDS_COLUMN_NAME]

    recording = rr.RecordingStream(f"bagel/{name}")
    recording.save(str(rrd_file))
    for signal in included:
        recording.send_columns(
            signal,
            indexes=[rr.TimeColumn("time", duration=times)],
            columns=rr.Scalars.columns(scalars=columns[signal]),
        )
    recording.flush(blocking=True)

    return {
        "rrd": str(rrd_file),
        "signals": included,
        "start_seconds": start_seconds,
        "end_seconds": end_seconds,
        "command": f"rerun '{rrd_file}'",
    }
