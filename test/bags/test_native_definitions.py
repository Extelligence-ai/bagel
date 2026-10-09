"""The native ROS 2 definition walker follows types nested in arrays and sequences.

`rosidl` describes ``Parameter[] new_parameters`` as a sequence wrapping a namespaced
type; a walker that only looks for bare namespaced types leaves ``Parameter`` (and
everything under it) out of the full text, and the schema parser then fails on it.
The real `rosidl` is apt-only, so the walk runs here against stand-ins shaped like it.
"""

from __future__ import annotations

import pathlib
import types
from collections.abc import Iterator

import pytest

from bagel_mcp.bags import native_backend
from bagel_mcp.topic.ros2.ros2msg import parse


class _NamespacedType:
    def __init__(self, *parts: str) -> None:
        self._parts = parts

    def namespaced_name(self) -> tuple[str, ...]:
        return self._parts


class _NestedType:
    def __init__(self, value_type: object) -> None:
        self.value_type = value_type


class _BasicType:
    pass


MSG_FILES = {
    "rcl_interfaces/msg/ParameterEvent": (
        "builtin_interfaces/Time stamp\nstring node\nParameter[] new_parameters\n"
        "Parameter[] changed_parameters\nParameter[] deleted_parameters\n"
    ),
    "builtin_interfaces/msg/Time": "int32 sec\nuint32 nanosec\n",
    "rcl_interfaces/msg/Parameter": "string name\nParameterValue value\n",
    "rcl_interfaces/msg/ParameterValue": "uint8 type\nbool bool_value\nint64 integer_value\n",
}
SLOT_TYPES = {
    "rcl_interfaces/msg/ParameterEvent": [
        _NamespacedType("builtin_interfaces", "msg", "Time"),
        _BasicType(),
        _NestedType(_NamespacedType("rcl_interfaces", "msg", "Parameter")),
        _NestedType(_NamespacedType("rcl_interfaces", "msg", "Parameter")),
        _NestedType(_NamespacedType("rcl_interfaces", "msg", "Parameter")),
    ],
    "builtin_interfaces/msg/Time": [_BasicType(), _BasicType()],
    "rcl_interfaces/msg/Parameter": [
        _BasicType(),
        _NamespacedType("rcl_interfaces", "msg", "ParameterValue"),
    ],
    "rcl_interfaces/msg/ParameterValue": [_BasicType(), _BasicType(), _BasicType()],
}


@pytest.fixture
def stub_rosidl(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name, text in MSG_FILES.items():
        msg_file = tmp_path / f"{name}.msg"
        msg_file.parent.mkdir(parents=True, exist_ok=True)
        msg_file.write_text(text, encoding="utf-8")
    monkeypatch.setattr(
        native_backend,
        "rosidl_definition",
        types.SimpleNamespace(NamespacedType=_NamespacedType, AbstractNestedType=_NestedType),
    )
    monkeypatch.setattr(
        native_backend,
        "rosidl_utilities",
        types.SimpleNamespace(
            get_message=lambda name: types.SimpleNamespace(SLOT_TYPES=SLOT_TYPES[name])
        ),
    )
    monkeypatch.setattr(
        native_backend,
        "rosidl_runtime",
        types.SimpleNamespace(get_interface_path=lambda name: str(tmp_path / f"{name}.msg")),
    )
    native_backend.locally_installed_ros2msg.cache_clear()
    yield
    native_backend.locally_installed_ros2msg.cache_clear()


@pytest.mark.usefixtures("stub_rosidl")
def test_types_nested_in_sequences_are_part_of_the_full_text() -> None:
    text = native_backend.locally_installed_ros2msg("rcl_interfaces/msg/ParameterEvent")

    assert text.count("MSG: ") == len(MSG_FILES) - 1  # the three dependencies
    assert "MSG: rcl_interfaces/msg/Parameter\n" in text
    assert "MSG: rcl_interfaces/msg/ParameterValue\n" in text
    main, dependencies = parse.parse(text)
    by_name = {field.name: field for field in main.fields}
    assert by_name["new_parameters"].type_ == "rcl_interfaces/msg/Parameter"
    assert by_name["new_parameters"].is_array
    assert set(dependencies) == {
        "builtin_interfaces/msg/Time",
        "rcl_interfaces/msg/Parameter",
        "rcl_interfaces/msg/ParameterValue",
    }


@pytest.mark.usefixtures("stub_rosidl")
def test_short_type_names_resolve_to_the_msg_namespace() -> None:
    text = native_backend.locally_installed_ros2msg("rcl_interfaces/ParameterEvent")

    assert text.startswith("builtin_interfaces/Time stamp")
