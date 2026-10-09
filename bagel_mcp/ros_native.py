"""Native ROS Python bindings: present in the ROS service images, absent on a pip install.

``rosbag``, ``genpy`` and ``cv_bridge`` (ROS 1) and ``rosbag2_py``, ``rclpy`` and
``rosidl_runtime_py`` (ROS 2) are apt packages sourced from ``/opt/ros``, not wheels
on PyPI, so ``pip install bagel-mcp`` cannot provide them. The modules that need them
bind the import through :func:`optional`, which keeps them importable without ROS, and
call :func:`require` from their ``register()`` so that, without ROS, they never enter
the DI registry or the pipeline capability list. Whatever reaches them anyway gets a
:class:`NativeRosUnavailableError` naming the Docker image to use instead.

ROS 2 MCAP bags are unaffected: Bagel reads those with its own MCAP reader.
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
        f"not part of a pip install. Run Bagel from the `{image}` Docker image instead "
        f"(`docker compose run --service-ports {image}`; see the README Quickstart). "
        "ROS 2 MCAP bags do not need it: Bagel reads them with its own MCAP reader."
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
