"""Native ROS Python bindings: present in the ROS service images, absent on a pip install.

``rosbag`` and ``genpy`` (ROS 1) and ``rosbag2_py``, ``rclpy`` and ``rosidl_runtime_py``
(ROS 2) are apt packages sourced from ``/opt/ros``, not wheels on PyPI. Bag files no
longer need them: the ``ros`` extra's pure-Python backend reads and writes them (see
:mod:`bagel_mcp.bags`). The native bag backend binds them through :func:`optional`,
which keeps it importable without ROS, and whatever reaches a missing binding gets a
:class:`NativeRosUnavailableError` naming the alternatives.
"""

import importlib
from typing import Any

ROS1_IMAGE = "ros1-noetic"
ROS1_CV_IMAGE = "ros1-noetic-cv"
ROS2_IMAGE = "ros2-kilted"

# Which service image ships each native package.
_IMAGES = {
    "rosbag": ROS1_IMAGE,
    "genpy": ROS1_IMAGE,
    "cv_bridge": ROS1_CV_IMAGE,
    "sensor_msgs.msg": ROS1_CV_IMAGE,
    "rosbag2_py": ROS2_IMAGE,
    "rclpy.serialization": ROS2_IMAGE,
    "rosidl_parser.definition": ROS2_IMAGE,
    "rosidl_runtime_py": ROS2_IMAGE,
    "rosidl_runtime_py.utilities": ROS2_IMAGE,
}


class NativeRosUnavailableError(ImportError):
    """A feature needs native ROS bindings that this environment does not have."""


def _message(name: str, feature: str) -> str:
    image = _IMAGES.get(name, ROS2_IMAGE)
    return (
        f"{feature} needs the native ROS package '{name}', which is not on PyPI and so "
        f"not part of a pip install. Bag files need no native ROS: install the `ros` extra "
        f"(`pip install 'bagel-mcp[ros]'`) or leave BAG_BACKEND on auto. For live ROS "
        f"topics run the `{image}` Docker image (`docker compose run --service-ports "
        f"{image}`; see the README Quickstart)."
    )


class _Missing:
    """Stands in for a native module that failed to import; any attribute access raises."""

    def __init__(self, name: str, feature: str) -> None:
        self._name = name
        self._feature = feature

    def __getattr__(self, attribute: str) -> Any:  # noqa: ANN401 -- raises, never returns
        raise NativeRosUnavailableError(_message(self._name, self._feature))

    def __repr__(self) -> str:
        return f"<native ROS module {self._name!r} unavailable>"


def optional(name: str, *, feature: str) -> Any:  # noqa: ANN401 -- a module, or its stand-in
    """Import a native ROS module if present, else return a stand-in that raises on use.

    Args:
        name (str): Dotted module name, e.g. ``"rosbag2_py"``.
        feature (str): What the caller does with it, for the error message.

    Returns:
        The imported module, or a :class:`_Missing` stand-in.

    """
    try:
        return importlib.import_module(name)
    except ImportError:
        return _Missing(name, feature)


def available(name: str) -> bool:
    """Return whether the native module imports in this environment."""
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


def require(name: str, *, feature: str) -> None:
    """Raise :class:`NativeRosUnavailableError` unless the native module imports.

    Called from a module's ``register()`` so that, without ROS, the module is never
    registered for dependency injection and never listed as an available pipeline
    capability.
    """
    try:
        importlib.import_module(name)
    except ImportError as error:
        raise NativeRosUnavailableError(_message(name, feature)) from error
