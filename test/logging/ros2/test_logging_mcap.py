import os

import pytest

from src.logging.ros2.mcap import LoggingDataset
from src.source.ros2.mcap import SourceFactory
from src.topic.ros2.mcap import TopicRegistry

ROS_DISTRO = os.getenv("ROS_DISTRO")


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap_zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.to_df().shape == (6, 3)


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap_zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1758921828.5230587
    )

    # THEN
    assert relation.to_df().shape == (3, 3)
