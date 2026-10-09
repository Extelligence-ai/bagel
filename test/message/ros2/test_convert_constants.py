"""Constants come from the definition, not from whatever the message class exposes.

rclpy renders a ``byte`` constant as one-byte ``bytes``, rosbags as an int and the
mcap dynamic classes not at all; the column is typed from the definition, so the
value must be too, identically for every backend.
"""

from __future__ import annotations

import types

from bagel_mcp import bags
from bagel_mcp.message.ros2 import convert
from bagel_mcp.topic.ros2.ros2msg import parse, schema

LOG_DEFINITION = """\
byte DEBUG=10
byte INFO=20
uint8 level
string msg
"""


def _struct() -> object:
    main, deps = parse.parse(LOG_DEFINITION)
    return schema.to_pa_struct(main, deps)


def test_byte_constants_decode_as_ints_whatever_the_class_exposes() -> None:
    rclpy_like = types.SimpleNamespace(DEBUG=b"\n", INFO=b"\x14", level=20, msg="hi")
    rosbags_like = types.SimpleNamespace(DEBUG=10, INFO=20, level=20, msg="hi")
    bare = types.SimpleNamespace(level=20, msg="hi")  # mcap's dynamic classes

    expected = {"DEBUG": 10, "INFO": 20, "level": 20, "msg": "hi"}
    assert convert.to_json(rclpy_like, _struct()) == expected
    assert convert.to_json(rosbags_like, _struct()) == expected
    assert convert.to_json(bare, _struct()) == expected


def test_sample_rosout_constants_through_the_backend() -> None:
    reader = bags.open_reader("data/sample/ros2/mcap", 2)
    main, deps = parse.parse(reader.definition("/rosout"))
    struct = schema.to_pa_struct(main, deps)
    _, _, message = next(iter(reader.messages(["/rosout"], None, None)))
    decoded = convert.to_json(message, struct)
    assert (decoded["DEBUG"], decoded["FATAL"]) == (10, 50)
