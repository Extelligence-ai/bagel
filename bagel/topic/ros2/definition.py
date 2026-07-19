"""Representations of ROS2 message type definition."""

from typing import Literal

from pydantic import BaseModel


class Field(BaseModel):
    """Base class for field definitions."""

    name: str
    type_: str
    description: str | None


class Constant(Field):
    """Field definition for constant values."""

    value: bool | int | float | str


class BuiltInField(Field):
    """Field definition for built-in types."""

    is_array: bool
    array_size: int | None
    array_size_upper_bound: int | None


class BoolField(BuiltInField):
    """Field definition for boolean types."""

    type_: Literal["bool"]
    default: bool | list[bool] | None


class UintField(BuiltInField):
    """Field definition for unsigned integer types."""

    type_: Literal["uint8", "uint16", "uint32", "uint64"]
    default: int | list[int] | None


class IntField(BuiltInField):
    """Field definition for signed integer types."""

    type_: Literal["int8", "int16", "int32", "int64"]
    default: int | list[int] | None


class FloatField(BuiltInField):
    """Field definition for floating-point types."""

    type_: Literal["float32", "float64"]
    default: float | list[float] | None


class CharField(BuiltInField):
    """Field definition for char types (alias for uint8). Deprecated."""

    type_: Literal["char"]
    default: int | list[int] | None


class ByteField(BuiltInField):
    """Field definition for byte types (alias for int8). Deprecated."""

    type_: Literal["byte"]
    default: int | list[int] | None


class StringField(BuiltInField):
    """Field definition for string types."""

    type_: Literal["string", "wstring"]
    default: str | list[str] | None
    string_size_upper_bound: int | None


class ComplexField(Field):
    """Field definition for complex types."""

    is_array: bool
    array_size: int | None
    array_size_upper_bound: int | None


class Struct(BaseModel):
    """Struct definition that represents a complex type."""

    type_: str | None
    fields: list[Field]
