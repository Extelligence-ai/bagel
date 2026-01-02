import re
import textwrap

import lark
import pytest

from src.topic.ros2 import definition
from src.topic.ros2.ros2msg import parse


def test_should_parse_correct_bool_fields_and_constants() -> None:
    # GIVEN
    full_text = """
    bool name
    bool[] name
    bool[123] name
    bool[<=123] name
    bool name true
    bool[] name []
    bool[] name [true]
    bool[] name [true, False, 1]
    bool[3] name [true, true, true]
    bool[<=123] name [true, true, true]
    bool NAME=true
    bool NAME=True
    bool NAME=1
    bool NAME=false
    bool NAME=False
    bool NAME=0
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=True,
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[],
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[True],
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[True, False, True],
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=3,
            array_size_upper_bound=None,
            default=[True, True, True],
            description=None,
        ),
        definition.BoolField(
            name="name",
            type_="bool",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=[True, True, True],
            description=None,
        ),
        definition.Constant(name="NAME", type_="bool", value=True, description=None),
        definition.Constant(name="NAME", type_="bool", value=True, description=None),
        definition.Constant(name="NAME", type_="bool", value=True, description=None),
        definition.Constant(name="NAME", type_="bool", value=False, description=None),
        definition.Constant(name="NAME", type_="bool", value=False, description=None),
        definition.Constant(name="NAME", type_="bool", value=False, description=None),
    ]


def test_should_raise_if_bool_field_has_invalid_default_value() -> None:
    # GIVEN
    full_text = "bool NAME invalid"

    # WHEN / THEN
    with pytest.raises(lark.exceptions.UnexpectedCharacters):
        parse.parse(full_text)


def test_should_raise_if_bool_constant_has_invalid_value() -> None:
    # GIVEN
    full_text = "bool NAME = invalid"

    # WHEN / THEN
    with pytest.raises(lark.exceptions.UnexpectedCharacters):
        parse.parse(full_text)


def test_should_parse_correct_uint_fields_and_constants() -> None:
    # GIVEN
    full_text = """
    uint8 name
    uint16 name
    uint32 name
    uint64 name
    uint8[] name
    uint16[] name
    uint32[] name
    uint64[] name
    uint8[123] name
    uint16[123] name
    uint32[123] name
    uint64[123] name
    uint8[<=123] name
    uint16[<=123] name
    uint32[<=123] name
    uint64[<=123] name
    uint8 name 123
    uint8[] name [123]
    uint8[3] name [123, 123, 123]
    uint8[<=123] name [123, 132, 123]
    uint8 NAME=123
    uint16 NAME=123
    uint32 NAME=123
    uint64 NAME=123
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    struct.fields = [
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint16",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint32",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint64",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint16",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint64",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint16",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint32",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint64",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint16",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint64",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=123,
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[123],
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=True,
            array_size=3,
            array_size_upper_bound=None,
            default=[123, 123, 123],
            description=None,
        ),
        definition.UintField(
            name="name",
            type_="uint8",
            is_array=True,
            array_size=3,
            array_size_upper_bound=123,
            default=[123, 123, 123],
            description=None,
        ),
        definition.Constant(name="NAME", type_="uint8", value=123, description=None),
        definition.Constant(name="NAME", type_="uint16", value=123, description=None),
        definition.Constant(name="NAME", type_="uint32", value=123, description=None),
        definition.Constant(name="NAME", type_="uint64", value=123, description=None),
    ]


def test_should_raise_if_uint_field_default_value_is_out_of_range() -> None:
    # GIVEN
    full_text = "uint8 name 256"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match="uint8 default value 256 is out of range",
    ):
        parse.parse(full_text)

    # GIVEN
    full_text = "uint8[] name [256]"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match=re.escape("uint8 default value [256] is out of range"),
    ):
        parse.parse(full_text)


def test_should_raise_if_uint_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "uint8 NAME = 256"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidConstantError,
        match="uint8 value 256 is out of range",
    ):
        parse.parse(full_text)


def test_should_parse_correct_int_fields_and_constants() -> None:
    # GIVEN
    full_text = """
    int8 name
    int16 name
    int32 name
    int64 name
    int8[] name
    int16[] name
    int32[] name
    int64[] name
    int8[123] name
    int16[123] name
    int32[123] name
    int64[123] name
    int8[<=123] name
    int16[<=123] name
    int32[<=123] name
    int64[<=123] name
    int8 name 123
    int8[] name [123]
    int8[3] name [123, 123, 123]
    int8[<=123] name [123, 123, 123]
    int8 NAME=123
    int16 NAME=123
    int32 NAME=123
    int64 NAME=123
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    struct.fields = [
        definition.IntField(
            name="name",
            type_="int8",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int16",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int32",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int64",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int8",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int16",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int64",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int8",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int16",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int32",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int64",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int8",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int16",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int64",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int8",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=123,
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int8",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[123],
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int8",
            is_array=True,
            array_size=3,
            array_size_upper_bound=None,
            default=[123, 123, 123],
            description=None,
        ),
        definition.IntField(
            name="name",
            type_="int8",
            is_array=True,
            array_size=3,
            array_size_upper_bound=123,
            default=[123, 123, 123],
            description=None,
        ),
        definition.Constant(name="NAME", type_="int8", value=123, description=None),
        definition.Constant(name="NAME", type_="int16", value=123, description=None),
        definition.Constant(name="NAME", type_="int32", value=123, description=None),
        definition.Constant(name="NAME", type_="int64", value=123, description=None),
    ]


def test_should_raise_if_int_field_default_value_is_out_of_range() -> None:
    # GIVEN
    full_text = "int8 name 128"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match="int8 default value 128 is out of range",
    ):
        parse.parse(full_text)

    # GIVEN
    full_text = "int8[] name [128]"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match=re.escape("int8 default value [128] is out of range"),
    ):
        parse.parse(full_text)


def test_should_raise_if_int_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "int8 NAME = 128"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidConstantError,
        match="int8 value 128 is out of range",
    ):
        parse.parse(full_text)


def test_should_parse_correct_float_fields_and_constants() -> None:
    # GIVEN
    full_text = """
    float32 name
    float64 name
    float32[] name
    float64[] name
    float32[123] name
    float64[123] name
    float32[<=123] name
    float64[<=123] name
    float32 name 123.456
    float32[] name [123.456]
    float32[3] name [123.456, 123.456, 123.456]
    float32[<=123] name [123.456, 123.456, 123.456]
    float32 NAME=123.456
    float64 NAME=123.456
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float64",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float64",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float64",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float64",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=123.456,
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[123.456],
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=True,
            array_size=3,
            array_size_upper_bound=None,
            default=[123.456, 123.456, 123.456],
            description=None,
        ),
        definition.FloatField(
            name="name",
            type_="float32",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=[123.456, 123.456, 123.456],
            description=None,
        ),
        definition.Constant(name="NAME", type_="float32", value=123.456, description=None),
        definition.Constant(name="NAME", type_="float64", value=123.456, description=None),
    ]


def test_should_raise_if_float_field_default_value_is_out_of_range() -> None:
    # GIVEN
    full_text = "float32 name 3.4028236e+38"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match=re.escape("float32 default value 3.4028236e+38 is out of range"),
    ):
        parse.parse(full_text)

    # GIVEN
    full_text = "float32[] name [3.4028236e+38]"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match=re.escape("float32 default value [3.4028236e+38] is out of range"),
    ):
        parse.parse(full_text)


def test_should_raise_if_float_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "float32 NAME = 3.4028236e+38"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidConstantError,
        match=re.escape("float32 value 3.4028236e+38 is out of range"),
    ):
        parse.parse(full_text)


def test_should_parse_correct_char_fields_and_constants() -> None:
    # GIVEN
    full_text = """
    char name
    char[] name
    char[123] name
    char[<=123] name
    char name 123
    char[] name [123]
    char[3] name [123, 123, 123]
    char[<=123] name [123, 123, 123]
    char NAME=123
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.CharField(
            name="name",
            type_="char",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.CharField(
            name="name",
            type_="char",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.CharField(
            name="name",
            type_="char",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.CharField(
            name="name",
            type_="char",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.CharField(
            name="name",
            type_="char",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=123,
            description=None,
        ),
        definition.CharField(
            name="name",
            type_="char",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[123],
            description=None,
        ),
        definition.CharField(
            name="name",
            type_="char",
            is_array=True,
            array_size=3,
            array_size_upper_bound=None,
            default=[123, 123, 123],
            description=None,
        ),
        definition.CharField(
            name="name",
            type_="char",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=[123, 123, 123],
            description=None,
        ),
        definition.Constant(name="NAME", type_="char", value=123, description=None),
    ]


def test_should_raise_if_char_field_default_value_is_out_of_range() -> None:
    # GIVEN
    full_text = "char name 256"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match="char default value 256 is out of range",
    ):
        parse.parse(full_text)

    # GIVEN
    full_text = "char[] name [256]"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match=re.escape("char default value [256] is out of range"),
    ):
        parse.parse(full_text)


def test_should_raise_if_char_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "char NAME = 256"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidConstantError,
        match="char value 256 is out of range",
    ):
        parse.parse(full_text)


def test_should_parse_correct_byte_fields_and_constants() -> None:
    # GIVEN
    full_text = """
    byte name
    byte[] name
    byte[123] name
    byte[<=123] name
    byte name 123
    byte[] name [123]
    byte[3] name [123, 123, 123]
    byte[<=123] name [123, 123, 123]
    byte NAME=123
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=True,
            array_size=123,
            array_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=None,
            description=None,
        ),
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            default=123,
            description=None,
        ),
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            default=[123],
            description=None,
        ),
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=True,
            array_size=3,
            array_size_upper_bound=None,
            default=[123, 123, 123],
            description=None,
        ),
        definition.ByteField(
            name="name",
            type_="byte",
            is_array=True,
            array_size=None,
            array_size_upper_bound=123,
            default=[123, 123, 123],
            description=None,
        ),
        definition.Constant(name="NAME", type_="byte", value=123, description=None),
    ]


def test_should_raise_if_byte_field_default_value_is_out_of_range() -> None:
    # GIVEN
    full_text = "byte name 128"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match="byte default value 128 is out of range",
    ):
        parse.parse(full_text)

    # GIVEN
    full_text = "byte[] name [128]"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidDefaultValueError,
        match=re.escape("byte default value [128] is out of range"),
    ):
        parse.parse(full_text)


def test_should_raise_if_byte_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "byte NAME = 128"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidConstantError,
        match="byte value 128 is out of range",
    ):
        parse.parse(full_text)


def test_should_parse_correct_string_fields() -> None:
    # GIVEN
    full_text = """
    string name
    string<=10 name
    string[<=5] name
    string<=10[] name
    string<=10[<=5] name
    string name "hello"
    string name 'hello'
    string name "'hello'"
    string name '"hello"'
    string name "\\"hello\\""
    string name '\\'hello\\''
    string name \\foo
    string FOO_STR = 'Foo'    
    string EMPTY=
    string EXAMPLE="#comments" # are handled properly
    string UNQUOTED= Bar
    string UNQUOTEDSPACE = Bar Foo
    string UNQUOTEDSPECIAL = afse_doi@f4!  :834$%G$%
    string BLANK=
    string BLANKCOMMENT=# Blank with comment
    string BLANKSPACECOMMENT= # Blank with comment after space
    string ESCAPED_QUOTE = \\'a#comment
    """  # noqa: W291

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields == [
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=10,
            default=None,
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=True,
            array_size=None,
            array_size_upper_bound=5,
            string_size_upper_bound=None,
            default=None,
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=True,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=10,
            default=None,
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=True,
            array_size=None,
            array_size_upper_bound=5,
            string_size_upper_bound=10,
            default=None,
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default="hello",
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default="hello",
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default="'hello'",
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default='"hello"',
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default='"hello"',
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default="'hello'",
            description=None,
        ),
        definition.StringField(
            name="name",
            type_="string",
            is_array=False,
            array_size=None,
            array_size_upper_bound=None,
            string_size_upper_bound=None,
            default=r"\foo",
            description=None,
        ),
        definition.Constant(
            name="FOO_STR",
            type_="string",
            value="Foo",
            description=None,
        ),
        definition.Constant(
            name="EMPTY",
            type_="string",
            value="",
            description=None,
        ),
        definition.Constant(
            name="EXAMPLE",
            type_="string",
            value="#comments",
            description="are handled properly",
        ),
        definition.Constant(
            name="UNQUOTED",
            type_="string",
            value="Bar",
            description=None,
        ),
        definition.Constant(
            name="UNQUOTEDSPACE",
            type_="string",
            value="Bar Foo",
            description=None,
        ),
        definition.Constant(
            name="UNQUOTEDSPECIAL",
            type_="string",
            value="afse_doi@f4!  :834$%G$%",
            description=None,
        ),
        definition.Constant(
            name="BLANK",
            type_="string",
            value="",
            description=None,
        ),
        definition.Constant(
            name="BLANKCOMMENT",
            type_="string",
            value="",
            description="Blank with comment",
        ),
        definition.Constant(
            name="BLANKSPACECOMMENT",
            type_="string",
            value="",
            description="Blank with comment after space",
        ),
        definition.Constant(
            name="ESCAPED_QUOTE",
            type_="string",
            value="\\'a",
            description="comment",
        ),
    ]
