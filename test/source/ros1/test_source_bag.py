import pytest

from src.source.ros1 import bag


def test_source_factory() -> None:
    # GIVEN
    factory = bag.SourceFactory("data/sample/ros1/sample.bag")

    # WHEN
    metadata = factory.metadata

    # THEN
    assert factory.total_message_count == 34
    assert factory.start_seconds == 1660676075.8220897
    assert factory.end_seconds == 1660676084.8992772
    assert factory.version == "2.0"
    assert factory.indexed is True
    assert factory.compression == "none"
    assert metadata == {
        "total_message_count": 34,
        "start_seconds": 1660676075.8220897,
        "end_seconds": 1660676084.8992772,
        "duration_seconds": 9.077187538146973,
        "path": "data/sample/ros1/sample.bag",
        "size_bytes": 12464,
        "version": 2.0,
        "duration": 9.077188,
        "start": 1660676075.82209,
        "end": 1660676084.899277,
        "size": 12464,
        "messages": 34,
        "indexed": True,
        "compression": "none",
        "types": [
            {"type": "geometry_msgs/Twist", "md5": "9f195f881246fdfa2798d1d3eebca84a"},
            {"type": "rosgraph_msgs/Log", "md5": "acffd30cd6b6de30f120938c17c593fb"},
        ],
        "topics": [
            {"topic": "/rosout", "type": "rosgraph_msgs/Log", "messages": 1},
            {
                "topic": "/turtle1/cmd_vel",
                "type": "geometry_msgs/Twist",
                "messages": 33,
                "frequency": 5.5086,
            },
        ],
    }


def test_validate_path_should_raise() -> None:
    # WHEN / THEN
    with pytest.raises(FileNotFoundError):
        bag.SourceFactory("data/sample/ros1/non_exist.bag")
