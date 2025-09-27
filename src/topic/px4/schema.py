"""Cast ULog.Data to PyArrow StructType."""

import logging
import re

import pyarrow as pa
from pyulog.core import ULog

from src.topic import base
from src.topic.px4 import parse

# This mapping is adapted from the "Data Types" section:
# https://docs.px4.io/main/en/dev_log/ulog_file_format
TYPE_STR_TO_PA_TYPE = {
    "int8_t": pa.int8(),
    "uint8_t": pa.uint8(),
    "int16_t": pa.int16(),
    "uint16_t": pa.uint16(),
    "int32_t": pa.int32(),
    "uint32_t": pa.uint32(),
    "int64_t": pa.int64(),
    "uint64_t": pa.uint64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "bool": pa.int8(),
    "char": pa.int8(),
}


def name_and_index(field_name: str) -> tuple[str, int | None]:
    """Parse field name and an optional array index.

    Args:
        field_name (str): The field name to parse, e.g., "xyz[4]", "xyz".

    Returns:
        tuple[str, int | None]: A tuple containing the field name and the array index, if any.

    """
    pattern = re.compile(r"^([a-zA-Z_]\w*)(?:\[(\d+)\])?(?:(?:\.[a-zA-Z_]\w*|\[\d+\])+)?$")
    match = pattern.match(field_name.strip())
    name, index = match.group(1), match.group(2)
    return name, int(index) if index else None


def to_pa_struct(data: ULog.Data, ver_sw: str | None, download_description: bool) -> pa.StructType:
    """Cast a ULog.Data to a PyArrow StructType.

    Args:
        data (ULog.Data): An instance of ULog.Data from a ULog.
        ver_sw (str | None): The commit SHA of the PX4-Autopilot repo from the
            `ULog.msg_info_dict["ver_sw"]`. This is used to fetch the field descriptions from the
            repo. If None, the default branch is used.
        download_description (bool): Whether to download field descriptions from the PX4-Autopilot
            repo. If False, descriptions will not be fetched.

    Returns:
        pa.StructType: A PyArrow StructType representing the ULog.Data.

    """
    msg_content, descriptions = None, {}

    if download_description:
        try:
            msg_content = parse.msg_definition(data.name, ver_sw, overwrite=False)
        except Exception as e:
            logging.warning(
                f"Failed to fetch .msg for {data.name} at {ver_sw or parse.DEFAULT_BRANCH}: {e}"
            )

    if msg_content:
        try:
            descriptions, _ = parse.descriptions(msg_content)
        except Exception as e:
            logging.warning(f"Failed to parse .msg content for type {data.name}: {e}")

    pa_fields = []
    for field_data in data.field_data:
        field_name, _ = name_and_index(field_data.field_name)
        description = descriptions.get(field_name)
        pa_fields.append(
            pa.field(
                name=field_data.field_name,
                type=TYPE_STR_TO_PA_TYPE[field_data.type_str],
                nullable=False,
                metadata={base.DESCRIPTION_KEY: description} if description else None,
            )
        )
    return pa.struct(pa_fields)
