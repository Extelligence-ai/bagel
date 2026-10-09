"""Cast message objects into JSON-serializable dictionaries."""

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from bagel_mcp.topic import base

PRIMITIVE_TYPE = bool | int | float | str


def _sequence(value: object) -> list:
    """Return a plain list for either a Python sequence or a numpy array (rosbags, rclpy)."""
    tolist = getattr(value, "tolist", None)
    return tolist() if callable(tolist) else list(value)


def _scalar(value: object) -> object:
    """Return a Python scalar for either a Python value or a numpy scalar (rosbags, rclpy)."""
    item = getattr(value, "item", None)
    return item() if callable(item) and not isinstance(value, bytes | str) else value


def to_json(
    message: object, schema: pa.DataType
) -> PRIMITIVE_TYPE | list[PRIMITIVE_TYPE] | dict[str, Any]:
    """Recursively cast a deserialized ROS 2 message into a JSON-serializable dictionary."""
    match schema:
        case pa.ListType() | pa.FixedSizeListType() | pa.LargeListType():
            return [to_json(item, schema.value_type) for item in _sequence(message)]

        case pa.StructType():
            result = {}
            for field in schema.fields:
                try:
                    value = getattr(message, field.name)
                except AttributeError:
                    # This must be a constant field.
                    value_str = field.metadata[base.DEFAULT_KEY.encode("utf-8")].decode("utf-8")
                    value = pc.cast(value_str, field.type).as_py()
                result[field.name] = to_json(value, field.type)
            return result

        case pa.DataType():
            return _scalar(message)
