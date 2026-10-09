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


def _constant(field: pa.Field) -> object | None:
    """Return a constant field's value from the definition, in the schema's type.

    The definition is the one source for constants: message classes expose them
    inconsistently (rclpy renders a ``byte`` constant as ``bytes``, rosbags as an
    int, mcap's dynamic classes not at all) while the column is typed from the
    definition.
    """
    metadata = field.metadata or {}
    value = metadata.get(base.DEFAULT_KEY.encode("utf-8"))
    if value is None:
        return None
    return pc.cast(value.decode("utf-8"), field.type).as_py()


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
                constant = _constant(field)
                if constant is not None:
                    value = constant
                else:
                    value = getattr(message, field.name)
                result[field.name] = to_json(value, field.type)
            return result

        case pa.DataType():
            return _scalar(message)
