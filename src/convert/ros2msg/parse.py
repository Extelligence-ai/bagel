"""Parsing function for ROS2 message type definition."""

import functools
import pathlib
import re
from collections.abc import Callable
from typing import Any

import lark

from src.convert.ros2msg import definition

TRUE_LITERALS = ("true", "True", "1")

FLOAT32_RANGE = (-3.4028235e38, 3.4028235e38)


class InvalidArrayTokenError(ValueError):
    """Raised when ARRAY token is invalid."""


class InvalidDefaultValueTokenError(ValueError):
    """Raised when the tokens for default value are invalid."""


class InvalidDefaultValueError(ValueError):
    """Raised when the default value is invalid for the field type."""


class InvalidConstantError(ValueError):
    """Raised when a constant is invalid."""


class TypeNameNotFoundError(Exception):
    """Raised when a type name is not found in the dependencies."""


def field_name(tokens: list[lark.lexer.Token]) -> str:
    """Return the field name from tokens."""
    return next(t.value for t in tokens if t.type == "FIELD_NAME")


def constant_name(tokens: list[lark.lexer.Token]) -> str:
    """Return the constant name from tokens."""
    return next(t.value for t in tokens if t.type == "CONSTANT_NAME")


def array_info(tokens: list[lark.lexer.Token]) -> dict[str, bool | int | None]:
    """Return array information from tokens."""
    match next((str(t.value) for t in tokens if t.type == "ARRAY"), None):
        case None:
            return {"is_array": False, "array_size": None, "array_size_upper_bound": None}

        case token if token.startswith("[") and token.endswith("]"):
            match token[1:-1]:
                case "":
                    array_size, array_size_upper_bound = None, None
                case text if text.isdigit():
                    array_size, array_size_upper_bound = int(text), None
                case _:
                    array_size, array_size_upper_bound = None, int(text.lstrip("<="))
            if array_size is not None and array_size <= 0:
                raise InvalidArrayTokenError(
                    f"Array size must be a positive integer, got {array_size}"
                )
            if array_size_upper_bound is not None and array_size_upper_bound <= 0:
                raise InvalidArrayTokenError(
                    f"Array size upper bound must be a positive integer, got {array_size_upper_bound}"  # noqa: E501
                )
            return {
                "is_array": True,
                "array_size": array_size,
                "array_size_upper_bound": array_size_upper_bound,
            }

        case token:
            raise InvalidArrayTokenError(f"Malformed ARRAY token: {token}")


def default_value(
    tokens: list[lark.lexer.Token],
    token_type: str,
    info: dict[str, bool | int | None],
    cast: Callable[[str], Any],
) -> str | list[Any] | None:
    """Return the default value of a BuiltInField from tokens."""
    match (
        next((i for i, t in enumerate(tokens) if t.type == "B_L"), None),
        next((j for j, t in enumerate(tokens) if t.type == "B_R"), None),
    ):
        case None, None:
            token = next((t for t in tokens if t.type == token_type), None)
            value = cast(token.value) if token else None
        case i, j if i is not None and j is not None and i < j:
            value_tokens = tokens[i + 1 : j]
            if not all(t.type == token_type for t in value_tokens):
                raise InvalidDefaultValueTokenError(
                    f"Expected only {token_type} tokens in array, got: {value_tokens}"
                )
            value = [cast(t.value) for t in value_tokens]
        case _:
            raise InvalidDefaultValueTokenError(f"Malformed default value tokens: {tokens}")

    match info["is_array"], info["array_size"], info["array_size_upper_bound"]:
        case True, size, None if (
            size is not None and isinstance(value, list) and len(value) != size
        ):
            raise InvalidDefaultValueTokenError(
                f"Expected default array size of {size}, got {len(value)}"
            )
        case True, None, bound if (
            bound is not None and isinstance(value, list) and len(value) > bound
        ):
            raise InvalidDefaultValueTokenError(
                f"Expected maximum default array size of {bound}, got {len(value)}"
            )
        case True, _, _ if value is None or isinstance(value, list):
            return value
        case False, None, None if value is None or not isinstance(value, list):
            return value
        case _:
            raise InvalidDefaultValueTokenError(
                f"Invalid default value {value} with array info {info}"
            )


def bool_field(tokens: list[lark.lexer.Token], type_: str = "bool") -> definition.BoolField:
    """Return a BoolField from tokens."""
    info = array_info(tokens)
    return definition.BoolField(
        name=field_name(tokens),
        type_=type_,
        default=default_value(tokens, "BOOL_VALUE", info, lambda s: s.lower() in TRUE_LITERALS),
        **info,
    )


def uint_field(tokens: list[lark.lexer.Token], type_: str) -> definition.UintField:
    """Return a UintField from tokens."""
    info = array_info(tokens)
    default = default_value(tokens, "INT", info, int)
    power = int(type_.lstrip("uint"))
    if (isinstance(default, int) and not (0 <= default <= 2**power - 1)) or (
        isinstance(default, list) and not all(0 <= v <= 2**power - 1 for v in default)
    ):
        raise InvalidDefaultValueError(f"{type_} default value {default} is out of range")
    return definition.UintField(name=field_name(tokens), type_=type_, default=default, **info)


def int_field(tokens: list[lark.lexer.Token], type_: str) -> definition.IntField:
    """Return an IntField from tokens."""
    info = array_info(tokens)
    default = default_value(tokens, "SIGNED_INT", info, int)
    power = int(type_.lstrip("int")) - 1
    if (isinstance(default, int) and not (-(2**power) <= default <= 2**power - 1)) or (
        isinstance(default, list) and not all(-(2**power) <= v <= 2**power - 1 for v in default)
    ):
        raise InvalidDefaultValueError(f"{type_} default value {default} is out of range")
    return definition.IntField(name=field_name(tokens), type_=type_, default=default, **info)


def float_field(tokens: list[lark.lexer.Token], type_: str) -> definition.FloatField:
    """Return a FloatField from tokens."""
    info = array_info(tokens)
    default = default_value(tokens, "FLOAT_VALUE", info, float)
    if type_ == "float32":
        if (
            isinstance(default, float) and not (FLOAT32_RANGE[0] <= default <= FLOAT32_RANGE[1])
        ) or (
            isinstance(default, list)
            and not all(FLOAT32_RANGE[0] <= v <= FLOAT32_RANGE[1] for v in default)
        ):
            raise InvalidDefaultValueError(f"{type_} default value {default} is out of range")
    return definition.FloatField(name=field_name(tokens), type_=type_, default=default, **info)


def char_field(tokens: list[lark.lexer.Token], type_: str = "char") -> definition.CharField:
    """Return a CharField from tokens."""
    info = array_info(tokens)
    default = default_value(tokens, "INT", info, int)
    if (isinstance(default, int) and not (0 <= default <= 2**8 - 1)) or (
        isinstance(default, list) and not all(0 <= v <= 2**8 - 1 for v in default)
    ):
        raise InvalidDefaultValueError(f"{type_} default value {default} is out of range")
    return definition.CharField(name=field_name(tokens), type_=type_, default=default, **info)


def byte_field(tokens: list[lark.lexer.Token], type_: str = "byte") -> definition.ByteField:
    """Return a ByteField from the given tokens."""
    info = array_info(tokens)
    default = default_value(tokens, "SIGNED_INT", info, int)
    if (isinstance(default, int) and not (-(2**7) <= default <= 2**7 - 1)) or (
        isinstance(default, list) and not all(-(2**7) <= v <= 2**7 - 1 for v in default)
    ):
        raise InvalidDefaultValueError(f"{type_} default value {default} is out of range")
    return definition.ByteField(name=field_name(tokens), type_=type_, default=default, **info)


def to_string(raw: str, max_size: int | None) -> str:
    """Cast a raw token string to a proper string."""
    s = raw.strip()
    for quote in ['"', "'"]:
        if s.startswith(quote) and s.endswith(quote):
            s = s[1:-1]
            m = re.search(rf"(?<!\\)(?<!\\\\){re.escape(quote)}", s)
            if m is not None:
                raise ValueError(f"String inner quotes not properly escaped: {s}")
            s = s.replace(f"\\{quote}", quote)
            break
    if max_size is not None and len(s) > max_size:
        raise ValueError(f"String exceeds maximum size {max_size}: {s}")
    return s


def string_upper_bound(tokens: list[lark.lexer.Token]) -> int | None:
    """Return the maximum size of a string field from tokens."""
    if token := next((str(t.value) for t in tokens if t.type == "BOUND"), None):
        stripped_token = token.lstrip("<=")
        if not stripped_token.isdigit():
            raise ValueError(f"Invalid token for integer conversion: {token}")
        return int(stripped_token)
    return None


def string_field(tokens: list[lark.lexer.Token], type_: str) -> definition.StringField:
    """Return a StringField from tokens."""
    info = array_info(tokens)
    upper_bound = string_upper_bound(tokens)
    cast = functools.partial(to_string, max_size=upper_bound)
    return definition.StringField(
        name=field_name(tokens),
        type_=type_,
        default=default_value(tokens, "STRING", info, cast),
        string_size_upper_bound=upper_bound,
        **info,
    )


def builtin_field(tokens: list[lark.lexer.Token]) -> definition.BuiltInField:  # noqa: PLR0911
    """Return a BuiltInField from tokens."""
    if type_ := next((t.value for t in tokens if t.type == "BOOL_TYPE"), None):
        return bool_field(tokens, type_)
    elif type_ := next((t.value for t in tokens if t.type == "UINT_TYPE"), None):
        return uint_field(tokens, type_)
    elif type_ := next((t.value for t in tokens if t.type == "INT_TYPE"), None):
        return int_field(tokens, type_)
    elif type_ := next((t.value for t in tokens if t.type == "FLOAT_TYPE"), None):
        return float_field(tokens, type_)
    elif type_ := next((t.value for t in tokens if t.type == "CHAR_TYPE"), None):
        return char_field(tokens, type_)
    elif type_ := next((t.value for t in tokens if t.type == "BYTE_TYPE"), None):
        return byte_field(tokens, type_)
    elif type_ := next((t.value for t in tokens if t.type == "STRING_TYPE"), None):
        return string_field(tokens, type_)
    else:
        raise ValueError(f"Failed to parse BuiltInField from tokens: {tokens}")


def complex_field(tokens: list[lark.lexer.Token]) -> definition.ComplexField:
    """Return a ComplexField from tokens."""
    return definition.ComplexField(
        name=field_name(tokens),
        type_=next(t.value for t in tokens if t.type == "COMPLEX_TYPE"),
        **array_info(tokens),
    )


def constant(tokens: list[lark.lexer.Token]) -> definition.Constant:  # noqa: C901, PLR0912
    """Return a Constant from tokens."""
    if (type_ := next((t.value for t in tokens if t.type == "BOOL_TYPE"), None)) and (
        text := next((t.value for t in tokens if t.type == "BOOL_VALUE"), None)
    ):
        value = text.lower() in TRUE_LITERALS

    elif (type_ := next((t.value for t in tokens if t.type == "UINT_TYPE"), None)) and (
        text := next((t.value for t in tokens if t.type == "INT"), None)
    ):
        value = int(text)
        power = int(type_.lstrip("uint"))
        if not (0 <= value <= 2**power - 1):
            raise InvalidConstantError(f"{type_} value {value} is out of range")

    elif (type_ := next((t.value for t in tokens if t.type == "INT_TYPE"), None)) and (
        text := next((t.value for t in tokens if t.type == "SIGNED_INT"), None)
    ):
        value = int(text)
        power = int(type_.lstrip("int")) - 1
        if not (-(2**power) <= value <= 2**power - 1):
            raise InvalidConstantError(f"{type_} value {value} is out of range")

    elif (type_ := next((t.value for t in tokens if t.type == "FLOAT_TYPE"), None)) and (
        text := next((t.value for t in tokens if t.type == "FLOAT_VALUE"), None)
    ):
        value = float(text)
        if type_ == "float32" and not (FLOAT32_RANGE[0] <= value <= FLOAT32_RANGE[1]):
            raise InvalidConstantError(f"{type_} value {value} is out of range")

    elif (type_ := next((t.value for t in tokens if t.type == "CHAR_TYPE"), None)) and (
        text := next((t.value for t in tokens if t.type == "INT"), None)
    ):
        value = int(text)
        if not (0 <= value <= 2**8 - 1):
            raise InvalidConstantError(f"{type_} value {value} is out of range")

    elif (type_ := next((t.value for t in tokens if t.type == "BYTE_TYPE"), None)) and (
        text := next((t.value for t in tokens if t.type == "SIGNED_INT"), None)
    ):
        value = int(text)
        if not (-(2**7) <= value <= 2**7 - 1):
            raise InvalidConstantError(f"{type_} value {value} is out of range")

    elif type_ := next((t.value for t in tokens if t.type == "STRING_TYPE"), None):
        text = next((t.value for t in tokens if t.type == "STRING"), None)
        max_size = string_upper_bound(tokens)
        value = to_string(text or "", max_size)

    else:
        raise ValueError(f"Failed to parse constant from tokens: {tokens}")

    return definition.Constant(name=constant_name(tokens), type_=type_, value=value)


###########################################################################
### Significant duplications of code with ros1msg/parse.py from here on ###
### This is tolerable as we might split code for into packages          ###
###########################################################################


def struct(lines: list[lark.tree.Tree]) -> definition.Struct:
    """Return a Struct from a group of lines."""
    type_name = None
    fields = []

    for line in lines:
        tokens = line.children
        match line.data:
            case "type":
                type_name = next(t.value for t in tokens if t.type == "COMPLEX_TYPE")
            case "builtin":
                fields.append(builtin_field(tokens))
            case "complex":
                fields.append(complex_field(tokens))
            case "constant":
                fields.append(constant(tokens))

    return definition.Struct(type_=type_name, fields=fields)


def fully_qualified_type_name(  # noqa: PLR0911
    type_name: str, namespace: str | None, dependencies: dict[str, definition.Struct]
) -> str:
    """Return the fully qualified type name for a given short type name."""
    if "/" in type_name and type_name in dependencies:
        return type_name  # already fully qualified
    elif type_name == "Header" and "std_msgs/Header" in dependencies:
        return "std_msgs/Header"  # http://wiki.ros.org/msg#Fields
    elif type_name == "builtin_interfaces/Time" and "builtin_interfaces/msg/Time" in dependencies:
        return "builtin_interfaces/msg/Time"
    elif (
        type_name == "builtin_interfaces/Duration"
        and "builtin_interfaces/msg/Duration" in dependencies
    ):
        return "builtin_interfaces/msg/Duration"
    elif (
        "/" in type_name
        and len(type_name.split("/")) == 2  # noqa: PLR2004
        and f"{type_name.split('/')[0]}/msg/{type_name.split('/')[1]}" in dependencies
    ):
        return f"{type_name.split('/')[0]}/msg/{type_name.split('/')[1]}"
    elif namespace and f"{namespace}/{type_name}" in dependencies:
        return f"{namespace}/{type_name}"
    elif struct_type := next((t for t in dependencies if t.endswith(f"/{type_name}")), None):
        return struct_type  # short type name omitting the namespace
    else:
        raise TypeNameNotFoundError(type_name)


def resolve_complex_field_types(
    main: definition.Struct, dependencies: dict[str, definition.Struct]
) -> definition.Struct:
    """Replace short type names in complex fields with fully qualified type names."""
    fields = []

    for field in main.fields:
        if isinstance(field, definition.ComplexField):
            namespace = "/".join(main.type_.split("/")[:-1]) if main.type_ else None
            resolved_type_name = fully_qualified_type_name(field.type_, namespace, dependencies)
            fields.append(field.model_copy(update={"type_": resolved_type_name}))
        else:
            fields.append(field)

    return main.model_copy(update={"fields": fields})


def parse(full_text: str) -> tuple[definition.Struct, dict[str, definition.Struct]]:
    """Parse a ROS2 message definition and return the main message struct and its dependencies.

    Args:
        full_text (str): a ROS2 message definition as a string.

    Returns:
        tuple[definition.Struct, dict[str, definition.Struct]]: Main message struct and a dictionary
            that maps dependency type names to their structs.

    """
    with open(pathlib.Path(__file__).parent / "grammar.lark") as f:
        grammar = f.read()

    parser = lark.Lark(grammar, parser="earley")

    groups: list[definition.Struct] = []

    lines = []

    for line in parser.parse(full_text + "\n").children:
        match line.data:
            case "separator":
                groups.append(struct(lines))
                lines = []

            case "comment" | "empty":
                continue

            case _:
                lines.append(line)

    if lines:
        groups.append(struct(lines))

    if not groups:
        return definition.Struct(type_=None, fields=[]), {}

    message = groups[0]

    dependencies = {s.type_: s for s in groups[1:]}

    message = resolve_complex_field_types(message, dependencies)

    dependencies = {
        t: resolve_complex_field_types(s, dependencies) for t, s in dependencies.items()
    }

    return message, dependencies
