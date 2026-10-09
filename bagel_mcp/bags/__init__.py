"""ROS bag file I/O behind one interface: see :mod:`bagel_mcp.bags.base` for the contract.

Backend selection (``settings.BAG_BACKEND``):

- ``auto`` (default): the pure-Python ``rosbags`` library when it is installed (the
  ``ros`` extra, present in every service image), else the native ROS stack of a
  service image, else :class:`BagSupportUnavailableError`.
- ``rosbags`` / ``native``: pin one; the parity tests run the same fixtures through both.
"""

from __future__ import annotations

import importlib
import importlib.util
import pathlib

from bagel_mcp.bags.base import (
    BagInfo,
    BagStorageError,
    BagSupportUnavailableError,
    FileInfo,
    Reader,
    TopicInfo,
    UnknownTopicError,
    Writer,
)
from bagel_mcp.settings import settings

__all__ = [
    "BagInfo",
    "BagStorageError",
    "BagSupportUnavailableError",
    "FileInfo",
    "Reader",
    "TopicInfo",
    "UnknownTopicError",
    "Writer",
    "available",
    "backend_name",
    "open_reader",
    "open_writer",
    "require",
]

ROS1_IMAGE = "ros1-noetic"
ROS2_IMAGE = "ros2-kilted"


def _rosbags_installed() -> bool:
    return importlib.util.find_spec("rosbags") is not None


def _native_installed(ros_version: int) -> bool:
    from bagel_mcp import ros_native

    return ros_native.available("rosbag" if ros_version == 1 else "rosbag2_py")


def backend_name(ros_version: int) -> str | None:
    """Return the backend ``settings.BAG_BACKEND`` resolves to, or None if none can serve."""
    choice = settings.BAG_BACKEND
    if choice == "rosbags":
        return "rosbags" if _rosbags_installed() else None
    if choice == "native":
        return "native" if _native_installed(ros_version) else None
    if choice != "auto":
        raise ValueError(f"BAG_BACKEND must be auto, rosbags or native, not {choice!r}")
    if _rosbags_installed():
        return "rosbags"
    if _native_installed(ros_version):
        return "native"
    return None


def available(ros_version: int) -> bool:
    """Return whether some backend can read bags of this ROS version."""
    return backend_name(ros_version) is not None


def require(feature: str, ros_version: int) -> None:
    """Raise :class:`BagSupportUnavailableError` unless a backend can serve ``feature``.

    Called from a module's ``register()`` so that, without bag support, the module is
    never registered for dependency injection and never listed as an available
    pipeline capability.
    """
    if backend_name(ros_version) is None:
        image = ROS1_IMAGE if ros_version == 1 else ROS2_IMAGE
        raise BagSupportUnavailableError(
            f"{feature} needs ROS bag support, which this install lacks. Add it with "
            f"`pip install 'bagel-mcp[ros]'` (`uv sync --extra ros` in a checkout), or run "
            f"the `{image}` Docker image, which also serves live ROS topics. "
            f"(BAG_BACKEND={settings.BAG_BACKEND!r}.)"
        )


def _backend(ros_version: int, feature: str) -> object:
    name = backend_name(ros_version)
    if name is None:
        require(feature, ros_version)
    return importlib.import_module(f"bagel_mcp.bags.{name}_backend")


def open_reader(path: str | pathlib.Path, ros_version: int) -> Reader:
    """Open a ROS 1 ``.bag`` or a ROS 2 bag (directory, ``.db3``, ``.mcap``) for reading."""
    backend = _backend(ros_version, f"Reading ROS {ros_version} bags")
    return backend.Reader(pathlib.Path(path), ros_version)


def open_writer(
    path: str | pathlib.Path,
    ros_version: int,
    storage: str = "sqlite3",
    compression: tuple[str, str] | str | None = None,
) -> Writer:
    """Create a bag for writing.

    Args:
        path: The ``.bag`` file (ROS 1) or the bag directory (ROS 2), which must not exist.
        ros_version: 1 or 2.
        storage: ROS 2 storage plugin, ``sqlite3`` or ``mcap``. Ignored for ROS 1.
        compression: ROS 2: ``("file" | "message", "zstd")``; ROS 1: ``"bz2"`` or ``"lz4"``.
            None writes uncompressed.

    """
    backend = _backend(ros_version, f"Writing ROS {ros_version} bags")
    return backend.Writer(pathlib.Path(path), ros_version, storage, compression)
