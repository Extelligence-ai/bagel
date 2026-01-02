import re
import textwrap

import lark
import pytest

from src.topic.ros1 import definition, parse


def test_should_parse_correct_bool_fields_and_constants() -> None:
    # GIVEN
    full_text = """
    bool name
    bool[] name
    bool[123] name
    bool name=true
    bool name=True
    bool name=1
    bool name=false
    bool name=False
    bool name=0
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields[0] == definition.BuiltInField(
        name="name", type_="bool", is_array=False, array_size=None, description=None
    )
    assert struct.fields[1] == definition.BuiltInField(
        name="name", type_="bool", is_array=True, array_size=None, description=None
    )
    assert struct.fields[2] == definition.BuiltInField(
        name="name", type_="bool", is_array=True, array_size=123, description=None
    )
    assert struct.fields[3] == definition.Constant(
        name="name", type_="bool", value=True, description=None
    )
    assert struct.fields[4] == definition.Constant(
        name="name", type_="bool", value=True, description=None
    )
    assert struct.fields[5] == definition.Constant(
        name="name", type_="bool", value=True, description=None
    )
    assert struct.fields[6] == definition.Constant(
        name="name", type_="bool", value=False, description=None
    )
    assert struct.fields[7] == definition.Constant(
        name="name", type_="bool", value=False, description=None
    )
    assert struct.fields[8] == definition.Constant(
        name="name", type_="bool", value=False, description=None
    )


def test_should_raise_if_bool_constant_has_invalid_value() -> None:
    # GIVEN
    full_text = "bool name = invalid"

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
    uint8 name=123
    uint16 name=123
    uint32 name=123
    uint64 name=123
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields[0] == definition.BuiltInField(
        name="name", type_="uint8", is_array=False, array_size=None, description=None
    )
    assert struct.fields[1] == definition.BuiltInField(
        name="name", type_="uint16", is_array=False, array_size=None, description=None
    )
    assert struct.fields[2] == definition.BuiltInField(
        name="name", type_="uint32", is_array=False, array_size=None, description=None
    )
    assert struct.fields[3] == definition.BuiltInField(
        name="name", type_="uint64", is_array=False, array_size=None, description=None
    )
    assert struct.fields[4] == definition.BuiltInField(
        name="name", type_="uint8", is_array=True, array_size=None, description=None
    )
    assert struct.fields[5] == definition.BuiltInField(
        name="name", type_="uint16", is_array=True, array_size=None, description=None
    )
    assert struct.fields[6] == definition.BuiltInField(
        name="name", type_="uint32", is_array=True, array_size=None, description=None
    )
    assert struct.fields[7] == definition.BuiltInField(
        name="name", type_="uint64", is_array=True, array_size=None, description=None
    )
    assert struct.fields[8] == definition.BuiltInField(
        name="name", type_="uint8", is_array=True, array_size=123, description=None
    )
    assert struct.fields[9] == definition.BuiltInField(
        name="name", type_="uint16", is_array=True, array_size=123, description=None
    )
    assert struct.fields[10] == definition.BuiltInField(
        name="name", type_="uint32", is_array=True, array_size=123, description=None
    )
    assert struct.fields[11] == definition.BuiltInField(
        name="name", type_="uint64", is_array=True, array_size=123, description=None
    )
    assert struct.fields[12] == definition.Constant(
        name="name", type_="uint8", value=123, description=None
    )
    assert struct.fields[13] == definition.Constant(
        name="name", type_="uint16", value=123, description=None
    )
    assert struct.fields[14] == definition.Constant(
        name="name", type_="uint32", value=123, description=None
    )
    assert struct.fields[15] == definition.Constant(
        name="name", type_="uint64", value=123, description=None
    )


def test_should_raise_if_uint_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "uint8 name = 256"

    # WHEN / THEN
    with pytest.raises(parse.InvalidConstantError, match="uint8 value 256 is out of range"):
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
    int8 name=123
    int16 name=123
    int32 name=123
    int64 name=123
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields[0] == definition.BuiltInField(
        name="name", type_="int8", is_array=False, array_size=None, description=None
    )
    assert struct.fields[1] == definition.BuiltInField(
        name="name", type_="int16", is_array=False, array_size=None, description=None
    )
    assert struct.fields[2] == definition.BuiltInField(
        name="name", type_="int32", is_array=False, array_size=None, description=None
    )
    assert struct.fields[3] == definition.BuiltInField(
        name="name", type_="int64", is_array=False, array_size=None, description=None
    )
    assert struct.fields[4] == definition.BuiltInField(
        name="name", type_="int8", is_array=True, array_size=None, description=None
    )
    assert struct.fields[5] == definition.BuiltInField(
        name="name", type_="int16", is_array=True, array_size=None, description=None
    )
    assert struct.fields[6] == definition.BuiltInField(
        name="name", type_="int32", is_array=True, array_size=None, description=None
    )
    assert struct.fields[7] == definition.BuiltInField(
        name="name", type_="int64", is_array=True, array_size=None, description=None
    )
    assert struct.fields[8] == definition.BuiltInField(
        name="name", type_="int8", is_array=True, array_size=123, description=None
    )
    assert struct.fields[9] == definition.BuiltInField(
        name="name", type_="int16", is_array=True, array_size=123, description=None
    )
    assert struct.fields[10] == definition.BuiltInField(
        name="name", type_="int32", is_array=True, array_size=123, description=None
    )
    assert struct.fields[11] == definition.BuiltInField(
        name="name", type_="int64", is_array=True, array_size=123, description=None
    )
    assert struct.fields[12] == definition.Constant(
        name="name", type_="int8", value=123, description=None
    )
    assert struct.fields[13] == definition.Constant(
        name="name", type_="int16", value=123, description=None
    )
    assert struct.fields[14] == definition.Constant(
        name="name", type_="int32", value=123, description=None
    )
    assert struct.fields[15] == definition.Constant(
        name="name", type_="int64", value=123, description=None
    )


def test_should_raise_if_int_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "int8 name = 128"

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
    float32 name=1.23
    float64 name=1.23
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields[0] == definition.BuiltInField(
        name="name", type_="float32", is_array=False, array_size=None, description=None
    )
    assert struct.fields[1] == definition.BuiltInField(
        name="name", type_="float64", is_array=False, array_size=None, description=None
    )
    assert struct.fields[2] == definition.BuiltInField(
        name="name", type_="float32", is_array=True, array_size=None, description=None
    )
    assert struct.fields[3] == definition.BuiltInField(
        name="name", type_="float64", is_array=True, array_size=None, description=None
    )
    assert struct.fields[4] == definition.BuiltInField(
        name="name", type_="float32", is_array=True, array_size=123, description=None
    )
    assert struct.fields[5] == definition.BuiltInField(
        name="name", type_="float64", is_array=True, array_size=123, description=None
    )
    assert struct.fields[6] == definition.Constant(
        name="name", type_="float32", value=1.23, description=None
    )
    assert struct.fields[7] == definition.Constant(
        name="name", type_="float64", value=1.23, description=None
    )


def test_should_raise_if_float_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "float32 name = 3.4028236e+38"

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
    char name=123
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields[0] == definition.BuiltInField(
        name="name", type_="char", is_array=False, array_size=None, description=None
    )
    assert struct.fields[1] == definition.BuiltInField(
        name="name", type_="char", is_array=True, array_size=None, description=None
    )
    assert struct.fields[2] == definition.BuiltInField(
        name="name", type_="char", is_array=True, array_size=123, description=None
    )
    assert struct.fields[3] == definition.Constant(
        name="name", type_="char", value=123, description=None
    )


def test_should_raise_if_char_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "char name = 256"

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
    byte name=65
    """

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields[0] == definition.BuiltInField(
        name="name", type_="byte", is_array=False, array_size=None, description=None
    )
    assert struct.fields[1] == definition.BuiltInField(
        name="name", type_="byte", is_array=True, array_size=None, description=None
    )
    assert struct.fields[2] == definition.BuiltInField(
        name="name", type_="byte", is_array=True, array_size=123, description=None
    )
    assert struct.fields[3] == definition.Constant(
        name="name", type_="byte", value=65, description=None
    )


def test_should_raise_if_byte_constant_is_out_of_range() -> None:
    # GIVEN
    full_text = "byte name = 256"

    # WHEN / THEN
    with pytest.raises(
        parse.InvalidConstantError,
        match="byte value 256 is out of range",
    ):
        parse.parse(full_text)


def test_should_parse_correct_string_fields() -> None:
    # GIVEN
    full_text = """
    string name
    string[] name
    string[123] name
    string name="Hello, World!"
    string name='Hello, World!'
    string name=Hello, World!
    string name=    Hello, World!    
    string name=Hello, World! # comment will be included
    string name=
    string name=    
    string name= # comment will be included
    string name=#
    """  # noqa: W291

    # WHEN
    struct, _ = parse.parse(textwrap.dedent(full_text))

    # THEN
    assert struct.fields[0] == definition.BuiltInField(
        name="name", type_="string", is_array=False, array_size=None, description=None
    )
    assert struct.fields[1] == definition.BuiltInField(
        name="name", type_="string", is_array=True, array_size=None, description=None
    )
    assert struct.fields[2] == definition.BuiltInField(
        name="name", type_="string", is_array=True, array_size=123, description=None
    )
    assert struct.fields[3] == definition.Constant(
        name="name", type_="string", value='"Hello, World!"', description=None
    )
    assert struct.fields[4] == definition.Constant(
        name="name", type_="string", value="'Hello, World!'", description=None
    )
    assert struct.fields[5] == definition.Constant(
        name="name", type_="string", value="Hello, World!", description=None
    )
    assert struct.fields[6] == definition.Constant(
        name="name", type_="string", value="Hello, World!", description=None
    )
    assert struct.fields[7] == definition.Constant(
        name="name",
        type_="string",
        value="Hello, World! # comment will be included",
        description=None,
    )
    assert struct.fields[8] == definition.Constant(
        name="name", type_="string", value="", description=None
    )
    assert struct.fields[9] == definition.Constant(
        name="name", type_="string", value="", description=None
    )
    assert struct.fields[10] == definition.Constant(
        name="name", type_="string", value="# comment will be included", description=None
    )
    assert struct.fields[11] == definition.Constant(
        name="name", type_="string", value="#", description=None
    )
