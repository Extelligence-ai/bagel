"""Parse and validate the manifest's `streams:` section (fleet streaming).

Two phases: `load_streams` turns raw YAML into models (shape validation);
`StreamsConfig.resolve` binds channel rules to a topic's Arrow schema
(field existence, scalar-ness, wire types). No runtime behavior lives here.
"""

import pyarrow as pa
import pyarrow.types as pat

from src.message.base import AccessPath
from src.sink.publish import StreamConfigError


def classify(pa_type: pa.DataType) -> str:
    """Wire-contract type name for a scalar Arrow type (spec §3)."""
    if pat.is_integer(pa_type) or pat.is_floating(pa_type):
        return "number"
    if pat.is_boolean(pa_type):
        return "bool"
    if pat.is_string(pa_type) or pat.is_large_string(pa_type):
        return "string"
    raise ValueError(f"{pa_type} is not a streamable scalar")


def resolve_path(struct: pa.StructType, dotted: str, *, field_label: str) -> AccessPath:
    """Walk a dotted path through nested structs to its leaf type."""
    current: pa.DataType = struct
    walked: list[str] = []
    for segment in dotted.split("."):
        if not pat.is_struct(current):
            raise StreamConfigError(
                field_label, f"'{'.'.join(walked)}' is not a struct; cannot descend to '{segment}'"
            )
        index = current.get_field_index(segment)
        if index == -1:
            raise StreamConfigError(field_label, f"unknown field '{segment}' in '{dotted}'")
        current = current.field(index).type
        walked.append(segment)
    return AccessPath(path=walked, pa_type=current)
