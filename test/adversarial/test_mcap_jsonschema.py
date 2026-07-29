"""Malformed jsonschema-encoded MCAP definitions: must never crash uncontrollably.

``jsonschema_to_struct`` (and its helper ``_jsonschema_type``) is the entry point
production code calls when an MCAP channel's schema encoding is "jsonschema"
(``TopicRegistry.struct`` / ``describe`` in ``src/topic/mcap.py``). A malformed
document -- non-UTF-8 bytes, non-JSON text, a non-object top level, type-confused
``properties``/``items``, or a pathologically deep nesting -- must raise the
module's clean, typed error (``base.UnsupportedEncodingError``), never a raw
``UnicodeDecodeError``, ``AttributeError``, ``RecursionError``, or ``TypeError``.
"""

import json

import pyarrow as pa
import pytest

from src.topic import base
from src.topic.mcap import jsonschema_to_struct


def _deeply_nested_object_bytes(depth: int) -> bytes:
    """Build JSON bytes for an object schema nested ``depth`` levels deep.

    Built via plain string concatenation (no Python-level recursion, no
    ``json.dumps``) so that constructing the *test fixture* itself never trips
    a ``RecursionError`` -- only feeding it through the parser under test
    should be able to do that.
    """
    prefix = '{"type": "object", "properties": {"child": ' * depth
    leaf = '{"type": "string"}'
    suffix = "}}" * depth
    return (prefix + leaf + suffix).encode()


CASES: dict[str, bytes] = {
    # Raw bytes that are not valid UTF-8 -- json.loads raises UnicodeDecodeError
    # (not json.JSONDecodeError) before it can even attempt to parse. Note:
    # b"\xff\xfe..." looks like it should qualify but is actually a UTF-16 LE
    # BOM that json.loads sniffs and decodes successfully (then fails as
    # JSONDecodeError, already handled) -- confirmed empirically. A lone
    # continuation byte with no BOM reliably fails UTF-8 decoding instead.
    "invalid_utf8": b'{"a": "\x80\x81"}',
    # Valid UTF-8, but not JSON at all.
    "non_json_text": b"not json at all {{{",
    # Valid JSON, but the top-level document is not an object.
    "valid_json_non_object_int": b"42",
    "valid_json_non_object_string": b'"just a string"',
    "valid_json_non_object_array": b"[1, 2, 3]",
    # Type confusion: "properties" is a str, not a dict -- .items() should not
    # be called on it.
    "properties_not_a_dict": json.dumps({"type": "object", "properties": "not-a-dict"}).encode(),
    # Nested well past any sane schema depth (150 levels), but still shallow
    # enough that json.loads() parses it fine -- this exercises
    # _jsonschema_type's own recursion-depth guard.
    "moderately_nested_exceeds_depth_cap": _deeply_nested_object_bytes(150),
    # Nested ~2000 levels deep -- deep enough that json.loads() itself hits
    # CPython's recursion limit while parsing, before _jsonschema_type is ever
    # called. This is a real, empirically-confirmed vector distinct from
    # _jsonschema_type's own recursion.
    "extremely_nested_breaks_json_loads": _deeply_nested_object_bytes(2000),
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_malformed_jsonschema_raises_clean_error(name: str) -> None:
    """Every malformed jsonschema definition must raise a clean, typed error.

    Specifically ``base.UnsupportedEncodingError`` -- not a raw
    ``UnicodeDecodeError``, ``AttributeError``, ``RecursionError``, or
    ``TypeError`` escaping from ``jsonschema_to_struct``.
    """
    with pytest.raises(base.UnsupportedEncodingError):
        jsonschema_to_struct(CASES[name])


def test_nested_items_not_a_dict_degrades_gracefully() -> None:
    """A nested ``"items"`` that is a str, not a dict, must not raise AttributeError.

    Unlike the other type-confusion cases, this one is nested one level below
    the top-level object, so the malformed ``items`` value doesn't invalidate
    the whole document -- it only affects the one field. Per the module's
    existing "schemaless object" convention (see
    ``test_jsonschema_mapper_edge_cases`` in
    ``test/pipeline/test_mcap_jsonschema.py``), the field degrades to a plain
    string (schemaless) rather than raising. This is a deliberate design
    choice, not an oversight -- this test guards it as a regression check.
    """
    definition = json.dumps(
        {
            "type": "object",
            "properties": {
                "bad_array": {"type": "array", "items": "not-a-dict"},
            },
        }
    ).encode()
    struct = jsonschema_to_struct(definition)
    assert isinstance(struct, pa.StructType)
    assert struct.field("bad_array").type == pa.list_(pa.string())


def test_property_not_a_dict_preserves_field_instead_of_dropping_it() -> None:
    """A property sub-schema that is a str, not a dict, must not silently vanish.

    Symmetric with ``test_nested_items_not_a_dict_degrades_gracefully``: a
    malformed ``items`` degrades to a schemaless string, so a malformed
    property sub-schema must do the same -- the field name is preserved and
    typed as ``pa.string()`` -- rather than being dropped from the struct with
    no error and no signal (the original bug this test guards against).
    """
    definition = json.dumps(
        {
            "type": "object",
            "properties": {
                "a": {"type": "number"},
                "b": "not-a-dict",
            },
        }
    ).encode()
    struct = jsonschema_to_struct(definition)
    assert isinstance(struct, pa.StructType)
    assert struct.names == ["a", "b"]
    assert struct.field("a").type == pa.float64()
    assert struct.field("b").type == pa.string()
