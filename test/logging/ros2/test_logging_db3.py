import os

import pytest

from src.logging.ros2.db3 import LoggingDataset
from src.source.ros2.db3 import SourceFactory
from src.topic.ros2.db3 import TopicRegistry

ROS_DISTRO = os.getenv("ROS_DISTRO")


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (6, 3)


@pytest.mark.skipif(
    ROS_DISTRO in ["iron", "humble"],
    reason=f"Skipping this test for ROS_DISTRO={ROS_DISTRO} due to lack of zstd support.",
)
def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1758921866.9605992
    )

    # THEN
    assert relation.shape == (2, 3)
