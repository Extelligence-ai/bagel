"""Synthetic ROS 1 and ROS 2 bags with numeric telemetry, written with the `rosbags` library.

The bundled sample bags carry only string topics, so they cannot drive the
event-windowed write paths. These helpers produce an IMU topic whose
``linear_acceleration.x`` dips below -10 in two distinct events (the ground truth
the tests assert against), plus a 1 Hz string topic to prove multi-topic copies.
They need no ROS installation, so the same fixtures serve the host suite, the
pip smoke test and the in-image parity tests.
"""

from __future__ import annotations

import math
import pathlib

import numpy as np
from rosbags.rosbag1 import Writer as Rosbag1Writer
from rosbags.rosbag2 import CompressionFormat, CompressionMode, StoragePlugin
from rosbags.rosbag2 import Writer as Rosbag2Writer
from rosbags.typesys import Stores, get_typestore

EPOCH = 1_700_000_000.0  # realistic, epoch-scale timestamps
SECOND_NS = 1_000_000_000

RATE_HZ = 20
DURATION_SECONDS = 12.0
CRUISE_ACCEL = -0.5
# (start offset, end offset, accel value): two hard decelerations below -10.
EVENTS = ((4.0, 5.0, -13.0), (8.0, 8.75, -11.5))
PREDICATE = "\"/imu\"['linear_acceleration']['x'] < -10"


def accel_at(offset_seconds: float) -> float:
    """The synthetic linear_acceleration.x profile at an offset into the bag."""
    for start, end, value in EVENTS:
        if start <= offset_seconds <= end:
            return value
    return CRUISE_ACCEL + 0.1 * math.sin(offset_seconds)


def event_onsets() -> list[float]:
    """Absolute timestamps of the rising edges (ground truth for assertions)."""
    return [EPOCH + start for start, _, _ in EVENTS]


def _samples() -> list[tuple[int, float, bool]]:
    """(timestamp_ns, accel_x, status_tick) per IMU sample."""
    count = int(DURATION_SECONDS * RATE_HZ)
    return [
        (int((EPOCH + i / RATE_HZ) * SECOND_NS), accel_at(i / RATE_HZ), i % RATE_HZ == 0)
        for i in range(count)
    ]


def write_ros2_imu_bag(
    directory: pathlib.Path,
    storage: str = "sqlite3",
    compression: tuple[str, str] | None = None,
    distro: str = "humble",
) -> pathlib.Path:
    """Write the synthetic ROS 2 bag (``sqlite3`` or ``mcap`` storage) and return its path.

    ``compression`` is ``("file", "zstd")`` or ``("message", "zstd")``; ``distro`` picks
    the typestore the messages are serialized with (the bag records no ``ros_distro``
    field for the reader to disagree with).
    """
    typestore = get_typestore(getattr(Stores, f"ROS2_{distro.upper()}"))
    imu_type = "sensor_msgs/msg/Imu"
    string_type = "std_msgs/msg/String"
    Imu = typestore.types[imu_type]  # noqa: N806 -- message classes
    String = typestore.types[string_type]  # noqa: N806
    Header = typestore.types["std_msgs/msg/Header"]  # noqa: N806
    Time = typestore.types["builtin_interfaces/msg/Time"]  # noqa: N806
    Vector3 = typestore.types["geometry_msgs/msg/Vector3"]  # noqa: N806
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]  # noqa: N806

    plugin = {"sqlite3": StoragePlugin.SQLITE3, "mcap": StoragePlugin.MCAP}[storage]
    writer = Rosbag2Writer(directory, version=8, storage_plugin=plugin)
    if compression:
        mode, fmt = compression
        writer.set_compression(CompressionMode[mode.upper()], CompressionFormat[fmt.upper()])
    with writer:
        imu = writer.add_connection("/imu", imu_type, typestore=typestore)
        status = writer.add_connection("/status", string_type, typestore=typestore)
        for timestamp_ns, accel_x, tick in _samples():
            message = Imu(
                header=Header(
                    stamp=Time(sec=timestamp_ns // SECOND_NS, nanosec=timestamp_ns % SECOND_NS),
                    frame_id="base_link",
                ),
                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                orientation_covariance=np.zeros(9),
                angular_velocity=Vector3(x=0.0, y=0.0, z=0.0),
                angular_velocity_covariance=np.zeros(9),
                linear_acceleration=Vector3(x=accel_x, y=0.0, z=9.81),
                linear_acceleration_covariance=np.zeros(9),
            )
            writer.write(imu, timestamp_ns, typestore.serialize_cdr(message, imu_type))
            if tick:
                offset = (timestamp_ns / SECOND_NS) - EPOCH
                text = String(data=f"tick {offset:.0f}")
                writer.write(status, timestamp_ns, typestore.serialize_cdr(text, string_type))
    return directory


def write_ros1_imu_bag(path: pathlib.Path, compression: str | None = None) -> pathlib.Path:
    """Write the synthetic ROS 1 ``.bag`` (``compression``: None, ``bz2`` or ``lz4``)."""
    typestore = get_typestore(Stores.ROS1_NOETIC)
    imu_type = "sensor_msgs/msg/Imu"
    string_type = "std_msgs/msg/String"
    Imu = typestore.types[imu_type]  # noqa: N806 -- message classes
    String = typestore.types[string_type]  # noqa: N806
    Header = typestore.types["std_msgs/msg/Header"]  # noqa: N806
    Time = typestore.types["builtin_interfaces/msg/Time"]  # noqa: N806
    Vector3 = typestore.types["geometry_msgs/msg/Vector3"]  # noqa: N806
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]  # noqa: N806

    writer = Rosbag1Writer(path)
    if compression:
        writer.set_compression(Rosbag1Writer.CompressionFormat[compression.upper()])
    with writer:
        imu = writer.add_connection("/imu", imu_type, typestore=typestore)
        status = writer.add_connection("/status", string_type, typestore=typestore)
        for sequence, (timestamp_ns, accel_x, tick) in enumerate(_samples()):
            message = Imu(
                header=Header(
                    seq=sequence,
                    stamp=Time(sec=timestamp_ns // SECOND_NS, nanosec=timestamp_ns % SECOND_NS),
                    frame_id="base_link",
                ),
                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                orientation_covariance=np.zeros(9),
                angular_velocity=Vector3(x=0.0, y=0.0, z=0.0),
                angular_velocity_covariance=np.zeros(9),
                linear_acceleration=Vector3(x=accel_x, y=0.0, z=9.81),
                linear_acceleration_covariance=np.zeros(9),
            )
            writer.write(imu, timestamp_ns, typestore.serialize_ros1(message, imu_type))
            if tick:
                offset = (timestamp_ns / SECOND_NS) - EPOCH
                text = String(data=f"tick {offset:.0f}")
                writer.write(status, timestamp_ns, typestore.serialize_ros1(text, string_type))
    return path
