"""MessageConverter implementation for Betaflight messages."""

from typing import Any

import pyarrow as pa
from orangebox.types import Frame

from src.convert import converter
from src.convert.betaflight import cast


class MessageConverter(converter.MessageConverter):
    """Convert Betaflight frames to JSON-serializable dictionaries."""

    def __init__(self, type_name: str, yaml_string: str) -> None:
        """Initialize a Betaflight MessageConverter.

        Args:
            type_name (str): Betaflight frame type name (e.g., "I", "P").
            yaml_string (str): Key-value pairs of field names and format strings in YAML format.

        """
        self._type_name = type_name
        self._raw_schema = yaml_string
        self._pa_struct = cast.to_pa_struct(yaml_string)

    @property
    def type_name(self) -> str:
        """Return the type name (frame) of the Betaflight message."""
        return self._type_name

    @property
    def raw_schema(self) -> str:
        """Return the Betaflight message definition string."""
        return self._raw_schema

    @property
    def pa_struct(self) -> pa.StructType:
        """Return the pyarrow StructType that represents the Betaflight message schema."""
        return self._pa_struct

    def to_dict(self, message: Frame) -> dict[str, Any]:
        """Convert a Betaflight message to a JSON-serializable dictionary.

        The message should already be a dictionary with keys matching the schema.
        """
        return {
            field_name: field_value
            for field_name, field_value in zip(self.pa_struct.names, message.data, strict=False)
        }
