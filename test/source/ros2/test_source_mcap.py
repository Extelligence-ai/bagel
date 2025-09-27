import os

import pytest

from src.source.ros2 import mcap

ROS_DISTRO = os.getenv("ROS_DISTRO")


def test_should_build_mcap_directory() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap/")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 15


def test_should_build_mcap_file() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap/part_0.mcap")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 15


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_should_build_mcap_zstd_directory() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap_zstd/")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 6


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_should_build_mcap_zstd_file() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap_zstd/part_0.mcap.zstd")

    # WHEN
    bag = factory.build()

    # THEN
    assert isinstance(bag, mcap.McapRos2Bag)
    assert len(bag.mcap_files) == 1
    assert bag.metadata.message_count == 6
