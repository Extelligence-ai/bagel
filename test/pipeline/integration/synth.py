"""Synthesize a small ROS2 bag with numeric IMU telemetry and deceleration events.

The bundled sample bags contain only string topics, so they cannot exercise the
event-driven reduction paths. This helper writes a bag (sqlite3 or mcap storage)
with a sensor_msgs/Imu topic whose linear_acceleration.x dips below -10 in two
distinct events -- the ground truth the integration tests assert against.

Requires a ROS2 environment (rclpy / rosbag2_py); run inside a bagel ros2-* container.

Usage as a script:
    uv run python -m test.pipeline.integration.synth --directory ./data/synthetic --storage mcap
"""

import argparse
import math
import pathlib

EPOCH = 1_700_000_000.0  # realistic, epoch-scale timestamps
SECOND_NS = 1_000_000_000

RATE_HZ = 20
DURATION_SECONDS = 12.0
CRUISE_ACCEL = -0.5
# (start offset, end offset, accel value): two hard decelerations below -10.
EVENTS = ((4.0, 5.0, -13.0), (8.0, 8.75, -11.5))


def accel_at(offset_seconds: float) -> float:
    """The synthetic linear_acceleration.x profile at an offset into the bag."""
    for start, end, value in EVENTS:
        if start <= offset_seconds <= end:
            return value
    return CRUISE_ACCEL + 0.1 * math.sin(offset_seconds)


def event_onsets() -> list[float]:
    """Absolute timestamps of the rising edges (ground truth for assertions)."""
    return [EPOCH + start for start, _, _ in EVENTS]


def write_imu_bag(directory: pathlib.Path, storage_id: str) -> pathlib.Path:
    """Write the synthetic bag and return its path.

    Args:
        directory: Directory to create the bag in (must not exist).
        storage_id: rosbag2 storage plugin -- "sqlite3" (db3) or "mcap".

    """
    import rosbag2_py
    from rclpy.serialization import serialize_message
    from sensor_msgs.msg import Imu
    from std_msgs.msg import String

    writer = rosbag2_py.SequentialWriter()
    writer.open(
        rosbag2_py.StorageOptions(uri=str(directory), storage_id=storage_id),
        rosbag2_py.ConverterOptions("", ""),
    )
    writer.create_topic(
        rosbag2_py.TopicMetadata(
            id=0, name="/imu", type="sensor_msgs/msg/Imu", serialization_format="cdr"
        )
    )
    writer.create_topic(
        rosbag2_py.TopicMetadata(
            id=1, name="/status", type="std_msgs/msg/String", serialization_format="cdr"
        )
    )

    count = int(DURATION_SECONDS * RATE_HZ)
    for i in range(count):
        offset = i / RATE_HZ
        timestamp_ns = int((EPOCH + offset) * SECOND_NS)

        imu = Imu()
        imu.header.stamp.sec = timestamp_ns // SECOND_NS
        imu.header.stamp.nanosec = timestamp_ns % SECOND_NS
        imu.header.frame_id = "base_link"
        imu.linear_acceleration.x = accel_at(offset)
        imu.linear_acceleration.z = 9.81
        writer.write("/imu", serialize_message(imu), timestamp_ns)

        if i % RATE_HZ == 0:  # 1 Hz status topic to prove multi-topic copy
            status = String()
            status.data = f"tick {offset:.0f}"
            writer.write("/status", serialize_message(status), timestamp_ns)

    del writer  # close the bag (finalizes metadata.yaml)
    return directory


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=pathlib.Path, required=True)
    parser.add_argument("--storage", choices=("sqlite3", "mcap"), default="sqlite3")
    args = parser.parse_args()
    write_imu_bag(args.directory, args.storage)
    print(f"Wrote synthetic {args.storage} bag to {args.directory}")  # noqa: T201
    print(f"Event onsets at: {event_onsets()}")  # noqa: T201


if __name__ == "__main__":
    main()
