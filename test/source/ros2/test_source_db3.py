import os

import pytest

from src.source.ros2 import db3

ROS_DISTRO = os.getenv("ROS_DISTRO")


def test_should_build_db3_directory() -> None:
    # GIVEN
    factory = db3.SourceFactory("data/sample/ros2/db3/")

    # WHEN
    reader = factory.build()

    # THEN
    assert isinstance(reader, db3.rosbag2_py.SequentialReader)
    assert reader.has_next()


def test_should_build_db3_file() -> None:
    # GIVEN
    factory = db3.SourceFactory("data/sample/ros2/db3/part_0.db3")

    # WHEN
    reader = factory.build()

    # THEN
    assert isinstance(reader, db3.rosbag2_py.SequentialReader)
    assert reader.has_next()


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_should_raise_db3_zstd_directory() -> None:
    # GIVEN / WHEN / THEN
    with pytest.raises(ValueError):
        db3.SourceFactory("data/sample/ros2/db3_zstd/")


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_should_build_db3_zstd_file() -> None:
    # GIVEN
    factory = db3.SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")

    # WHEN
    reader = factory.build()

    # THEN
    assert isinstance(reader, db3.rosbag2_py.SequentialReader)
    assert reader.has_next()
