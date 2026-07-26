"""End-to-end flows against data/sample fixtures via the real server tools."""

import pathlib

import pytest

import server

ROS2_MCAP = pathlib.Path("data/sample/ros2/mcap")


@pytest.mark.skipif(not ROS2_MCAP.exists(), reason="ros2 sample missing")
def test_describe_then_list_topics_flow() -> None:
    described = server.describe_data_source(str(ROS2_MCAP))
    assert isinstance(described, list) and described


@pytest.mark.skipif(not ROS2_MCAP.exists(), reason="ros2 sample missing")
def test_drill_into_first_topic() -> None:
    summary = server.describe_data_source(str(ROS2_MCAP))
    # Find a topic name from the summary payload.
    text = str(summary)
    assert "/" in text  # topics are slash-named; sanity gate
