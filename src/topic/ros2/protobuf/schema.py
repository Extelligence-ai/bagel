"""Convert Protobuf message descriptor to PyArrow DataTypes."""

import pyarrow as pa
from google.protobuf.descriptor import Descriptor, FieldDescriptor


def cast_field_descriptor(descriptor: FieldDescriptor) -> pa.DataType:  # noqa: C901, PLR0912
    """Cast a Protobuf FieldDescriptor to its corresponding PyArrow DataType."""
    match descriptor.type:
        case FieldDescriptor.TYPE_BOOL:
            pa_type = pa.bool_()
        case FieldDescriptor.TYPE_BYTES:
            pa_type = pa.binary()
        case FieldDescriptor.TYPE_UINT32 | FieldDescriptor.TYPE_FIXED32:
            pa_type = pa.uint32()
        case (
            FieldDescriptor.TYPE_INT32 | FieldDescriptor.TYPE_SINT32 | FieldDescriptor.TYPE_SFIXED32
        ):
            pa_type = pa.int32()
        case FieldDescriptor.TYPE_UINT64 | FieldDescriptor.TYPE_FIXED64:
            pa_type = pa.uint64()
        case (
            FieldDescriptor.TYPE_INT64 | FieldDescriptor.TYPE_SINT64 | FieldDescriptor.TYPE_SFIXED64
        ):
            pa_type = pa.int64()
        case FieldDescriptor.TYPE_FLOAT:
            pa_type = pa.float32()
        case FieldDescriptor.TYPE_DOUBLE:
            pa_type = pa.float64()
        case FieldDescriptor.TYPE_ENUM:
            pa_type = pa.int32()
        case FieldDescriptor.TYPE_STRING:
            pa_type = pa.string()
        case FieldDescriptor.TYPE_MESSAGE:
            if descriptor.message_type.full_name in (
                "google.protobuf.Timestamp",  # '2018-07-11T03:58:12.813431Z'
                "google.protobuf.Duration",  # '93784.500600s'
            ):
                # The underlying data is a struct, but json_format.MessageToDict translates
                # these two types into special strings.
                pa_type = pa.string()
            else:
                pa_type = to_pa_struct(descriptor.message_type)
        case _:
            raise ValueError(
                f"Unsupported type {descriptor.type} for Protobuf field {descriptor.full_name}"
            )

    if descriptor.label == FieldDescriptor.LABEL_REPEATED:
        return pa.list_(pa_type)
    else:
        return pa_type


def to_pa_struct(descriptor: Descriptor) -> pa.StructType:
    """Convert a Protobuf message Descriptor to a PyArrow StructType."""
    pa_fields = []
    for proto_field in descriptor.fields:
        pa_type = cast_field_descriptor(proto_field)
        pa_fields.append(pa.field(proto_field.name, pa_type, nullable=True))
    return pa.struct(pa_fields)
