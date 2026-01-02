"""Cast message objects into JSON-serializable dictionaries."""

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from src.topic import base

PRIMITIVE_TYPE = bool | int | float | str

INT_TYPE_IDS = {
    pa.uint8().id,
    pa.uint16().id,
    pa.uint32().id,
    pa.uint64().id,
    pa.int8().id,
    pa.int16().id,
    pa.int32().id,
    pa.int64().id,
}


def to_json(
    message: PRIMITIVE_TYPE | list[PRIMITIVE_TYPE] | dict[str, Any], schema: pa.DataType
) -> PRIMITIVE_TYPE | list[PRIMITIVE_TYPE] | dict[str, Any]:
    """Recursively cast a Bagel message object into a JSON-serializable dictionary."""
    match schema:
        case pa.ListType() if schema.value_type.id in INT_TYPE_IDS and isinstance(message, str):
            return [ord(c) for c in message]

        case pa.ListType():
            return [to_json(item, schema.value_type) for item in message]

        case pa.StructType():
            result = {}
            for field in schema.fields:
                default = None
                if field.metadata is not None:
                    default_str = field.metadata.get(base.DEFAULT_KEY.encode("utf-8"))
                    default = (
                        pc.cast(default_str.decode("utf-8"), field.type).as_py()
                        if default_str is not None
                        else None
                    )
                value = message.get(field.name, default)
                result[field.name] = to_json(value, field.type) if value is not None else None
            return result

        case pa.DataType():
            return message
