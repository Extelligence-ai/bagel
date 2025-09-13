"""Parsing function for ROS1 message type definition."""

import pathlib

import lark

from src.topic.ros1 import definition

TRUE_LITERALS = ("true", "True", "1")

FLOAT32_RANGE = (-3.4028235e38, 3.4028235e38)


class InvalidArrayTokenError(ValueError):
    """Raised when ARRAY token is invalid."""


class InvalidConstantError(ValueError):
    """Raised when a constant is invalid."""


class TypeNameNotFoundError(Exception):
    """Raised when a type name is not found in the dependencies."""


def field_name(tokens: list[lark.lexer.Token]) -> str:
    """Return the field name from tokens."""
    return next(t.value for t in tokens if t.type == "NAME")


def array_info(tokens: list[lark.lexer.Token]) -> dict[str, bool | int | None]:
    """Return the array information from tokens."""
    match next((str(t.value) for t in tokens if t.type == "ARRAY"), None):
        case None:
            return {"is_array": False, "array_size": None}

        case token if token.startswith("[") and token.endswith("]"):
            size_text = token[1:-1]
            array_size = int(size_text) if size_text else None

            if array_size is not None and array_size <= 0:
                raise InvalidArrayTokenError(
                    f"Array size must be a positive integer, got {array_size}"
                )

            return {"is_array": True, "array_size": array_size}

        case token:
            raise InvalidArrayTokenError(f"Malformed ARRAY token: {token}")


def comment(tokens: list[lark.lexer.Token]) -> str | None:
    """Return the comment from tokens."""
    match [t.value for t in tokens if t.type == "COMMENT"]:
        case [s]:
            return str(s).lstrip("# ").rstrip()
        case []:
            return None
        case _:
            raise ValueError(f"Multiple COMMENT tokens found: {tokens}")


def builtin_field(tokens: list[lark.lexer.Token]) -> definition.BuiltInField:
    """Return a BuiltInField from tokens."""
    return definition.BuiltInField(
        name=field_name(tokens),
        type_=next(t.value for t in tokens if t.type == "BUILTIN_TYPE"),
        description=comment(tokens),
        **array_info(tokens),
    )


def complex_field(tokens: list[lark.lexer.Token]) -> definition.ComplexField:
    """Return a ComplexField from tokens."""
    return definition.ComplexField(
        name=field_name(tokens),
        type_=next(t.value for t in tokens if t.type == "COMPLEX_TYPE"),
        description=comment(tokens),
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
        value = text or ""
        value = value.strip()

    else:
        raise ValueError(f"Failed to parse constant from tokens: {tokens}")

    return definition.Constant(
        name=field_name(tokens),
        type_=type_,
        description=comment(tokens),
        value=value,
    )


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


def fully_qualified_type_name(
    type_name: str, namespace: str | None, dependencies: dict[str, definition.Struct]
) -> str:
    """Return the fully qualified type name for a given short type name."""
    if "/" in type_name and type_name in dependencies:
        return type_name  # already fully qualified
    elif type_name == "Header" and "std_msgs/Header" in dependencies:
        return "std_msgs/Header"  # http://wiki.ros.org/msg#Fields
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
    """Parse a ROS1 message definition and return the main message struct and its dependencies.

    Args:
        full_text (str): a ROS1 message definition as a string.

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
        if line.data == "separator":
            groups.append(struct(lines))
            lines = []
        else:
            lines.append(line)

    if lines:
        groups.append(struct(lines))

    if not groups:
        return definition.Struct(type_=None, fields=[]), {}

    main = groups[0]

    dependencies = {s.type_: s for s in groups[1:]}

    main = resolve_complex_field_types(main, dependencies)

    dependencies = {
        t: resolve_complex_field_types(s, dependencies) for t, s in dependencies.items()
    }

    return main, dependencies
