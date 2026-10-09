"""Cast message objects into JSON-serializable dictionaries."""

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from bagel_mcp.topic import base

PRIMITIVE_TYPE = bool | int | float | str

# genpy spells stamps `secs`/`nsecs`; rosbags' ROS 1 typestore (like ROS 2) spells
# them `sec`/`nanosec`. The schema keeps the ROS 1 names; both message flavors fit.
_FIELD_ALIASES = {"secs": "sec", "nsecs": "nanosec"}


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

    The definition is the one source for constants, whatever the message class
    (genpy or rosbags) exposes for them.
    """
    metadata = field.metadata or {}
    value = metadata.get(base.DEFAULT_KEY.encode("utf-8"))
    if value is None:
        return None
    return pc.cast(value.decode("utf-8"), field.type).as_py()


def to_json(
    message: object, schema: pa.DataType
) -> PRIMITIVE_TYPE | list[PRIMITIVE_TYPE] | dict[str, Any]:
    """Recursively cast a deserialized ROS 1 message into a JSON-serializable dictionary."""
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
                    try:
                        value = getattr(message, field.name)
                    except AttributeError:
                        value = getattr(message, _FIELD_ALIASES[field.name])
                result[field.name] = to_json(value, field.type)
            return result

        case pa.DataType():
            return _scalar(message)
