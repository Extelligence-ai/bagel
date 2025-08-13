"""Factory function to create a MessageConverter based on the schema encoding."""

import pathlib

from src.convert import converter, schema


def make_converter(robolog_path: str | pathlib.Path, type_name: str) -> converter.MessageConverter:
    """Create a MessageConverter based on the schema encoding of the message type in a robolog."""
    schema_string = schema.schema_string(robolog_path, type_name)

    match schema.schema_encoding(robolog_path, type_name):
        case schema.Encoding.ROS1MSG:
            from src.convert import ros1msg

            return ros1msg.MessageConverter(type_name, schema_string)

        case schema.Encoding.ROS2MSG:
            from src.convert import ros2msg

            return ros2msg.MessageConverter(type_name, schema_string)

        case schema.Encoding.PROTOBUF:
            from src.convert import protobuf

            return protobuf.MessageConverter(type_name, schema_string)

        case schema.Encoding.PX4ULOG:
            from src.convert import px4ulog

            return px4ulog.MessageConverter(type_name, schema_string)

        case schema.Encoding.ARDUPILOTBIN:
            from src.convert import ardupilotbin

            return ardupilotbin.MessageConverter(type_name, schema_string)

        case encoding:
            raise schema.UnsupportedSchemaEncodingError(encoding)
