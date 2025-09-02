"""Convert Betaflight format strings to PyArrow DataTypes."""

import pyarrow as pa
import yaml


def cast(signed: int | None) -> pa.DataType:
    """Cast a Betaflight signed value to a PyArrow DataType.

    The fields in the Betaflight frames are all integers:
    https://github.com/cleanflight/cleanflight/blob/master/src/main/blackbox/blackbox.c
    """
    return pa.uint64() if signed == 0 else pa.int64()


def to_pa_struct(yaml_string: str) -> pa.StructType:
    """Convert a YAML string representing a Betaflight frame schema to a PyArrow StructType.

    Args:
        yaml_string (str): Key-value pairs of field names and their format strings.

    Returns:
        pa.StructType: The corresponding PyArrow StructType.

    """
    schema = yaml.safe_load(yaml_string)
    fields = []
    for field_name, field_def in schema.items():
        fields.append(pa.field(field_name, cast(field_def["signed"]), nullable=True))
    return pa.struct(fields)
