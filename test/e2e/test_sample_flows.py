"""End-to-end flows against data/sample fixtures via the real server tools."""

import json
import pathlib
import re

import pytest

import server

ROS2_MCAP = pathlib.Path("data/sample/ros2/mcap")


@pytest.mark.skipif(not ROS2_MCAP.exists(), reason="ros2 sample missing")
def test_describe_then_list_topics_flow() -> None:
    described = server.describe_data_source(str(ROS2_MCAP))
    assert isinstance(described, list) and described


@pytest.mark.skipif(not ROS2_MCAP.exists(), reason="ros2 sample missing")
def test_drill_into_first_topic() -> None:
    """Extract a real topic name from describe_data_source and drill into it.

    ``describe_data_source`` returns a list of poml chat messages (each a
    dict with "speaker"/"content"); the "human" message's content embeds a
    fenced ```json block under a "# Available Topics in the Data Source"
    heading containing the actual list of topic-name strings (this comes
    straight from ``registry.available_topics(...)`` -- see server.py). We
    parse that block to get a real topic name, then call
    ``describe_topic`` -- the actual drill-down tool -- on it.
    """
    summary = server.describe_data_source(str(ROS2_MCAP))
    human_content = next(
        message["content"] for message in summary if message.get("speaker") == "human"
    )
    match = re.search(
        r"# Available Topics in the Data Source\s*```json\s*(\[.*?\])\s*```",
        human_content,
        re.DOTALL,
    )
    assert match is not None, f"could not find topics json block in: {human_content!r}"
    topics = json.loads(match.group(1))
    assert topics, "no topics found in describe_data_source output"
    topic = topics[0]

    described_topic = server.describe_topic(str(ROS2_MCAP), topic)
    assert isinstance(described_topic, list) and described_topic
