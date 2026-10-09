"""Representations of ROS1 message type definition."""

from typing import Literal

from pydantic import BaseModel


class Field(BaseModel):
    """Base class for field definitions."""

    name: str
    type_: str
    description: str | None


class Constant(Field):
    """Field definition for constant values."""

    type_: Literal[
        "bool",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "int8",
        "int16",
        "int32",
        "int64",
        "float32",
        "float64",
        "char",
        "byte",
        "string",
    ]
    value: bool | int | float | str


class BuiltInField(Field):
    """Field definition for built-in types."""

    type_: Literal[
        "bool",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "int8",
        "int16",
        "int32",
        "int64",
        "float32",
        "float64",
        "char",
        "byte",
        "string",
        "time",
        "duration",
    ]
    is_array: bool
    array_size: int | None


class ComplexField(Field):
    """Field definition for complex types."""

    is_array: bool
    array_size: int | None


class Struct(BaseModel):
    """Struct definition that represents a complex type."""

    type_: str | None
    fields: list[Field]
