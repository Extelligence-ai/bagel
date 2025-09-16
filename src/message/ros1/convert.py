"""Cast message objects into JSON-serializable dictionaries."""

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from src.topic import base

PRIMITIVE_TYPE = bool | int | float | str


def to_json(
    message: object, schema: pa.DataType
) -> PRIMITIVE_TYPE | list[PRIMITIVE_TYPE] | dict[str, Any]:
    """Recursively cast a deserialized ROS1 message into a JSON-serializable dictionary."""
    match schema:
        case pa.ListType():
            return [to_json(item, schema.value_type) for item in message]

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
            return message
