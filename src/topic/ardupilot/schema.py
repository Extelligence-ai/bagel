"""Cast ArduPilot DFFormat to PyArrow StructType."""

import pyarrow as pa
from pymavlink import DFReader

from src.topic import base

# This mapping is adapted from the AP_Logger's README:
# https://github.com/ArduPilot/ardupilot/blob/master/libraries/AP_Logger/README.md
FORMAT_CHARACTER_TO_PYARROW_TYPE = {
    "a": pa.string(),
    "b": pa.int8(),
    "B": pa.uint8(),
    "h": pa.int16(),
    "H": pa.uint16(),
    "i": pa.int32(),
    "I": pa.uint32(),
    "f": pa.float32(),
    "d": pa.float64(),
    "n": pa.string(),
    "N": pa.string(),
    "Z": pa.string(),
    "L": pa.int32(),
    "M": pa.uint8(),
    "q": pa.int64(),
    "Q": pa.uint64(),
    "g": pa.float16(),
    # Legacy formats
    "c": pa.int16(),
    "C": pa.uint16(),
    "e": pa.int32(),
    "E": pa.uint32(),
}


def to_pa_struct(fmt: DFReader.DFFormat, metadata: DFReader.DFMetaData) -> pa.StructType:
    """Convert an ArduPilot DFFormat to a PyArrow StructType.

    Args:
        fmt (DFReader.DFFormat): The DFFormat of a DFMessage.
        metadata (DFReader.DFMetaData): The metadata for all DFMessage types.

    Returns:
        pa.StructType: The converted PyArrow StructType.

    """
    descriptions = {
        f.get("name"): f.description.text for f in metadata.metadata_tree()[fmt.name].fields.field
    }

    pa_fields = []
    for name, char, units in zip(fmt.columns, fmt.msg_fmts, fmt.units, strict=True):
        pa_metadata = {}
        if units:
            pa_metadata[base.UNITS_KEY] = units
        if name in descriptions:
            pa_metadata[base.DESCRIPTION_KEY] = descriptions[name]
        pa_fields.append(
            pa.field(
                name=name,
                type=FORMAT_CHARACTER_TO_PYARROW_TYPE[char],
                nullable=False,
                metadata=pa_metadata if pa_metadata else None,
            )
        )

    return pa.struct(pa_fields)
