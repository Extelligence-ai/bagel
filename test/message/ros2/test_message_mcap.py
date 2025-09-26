from src.message.ros2.mcap import MessageDataset
from src.source.ros2.mcap import SourceFactory
from src.topic.ros2.mcap import TopicRegistry


def test_message_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (15, 5)


def test_can_select_topic() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["/fluid_pressure"])

    # THEN
    assert relation.shape == (7, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=1689969666.41214, end_seconds=1689969666.6120112
    )

    # THEN
    assert relation.shape == (2, 5)


def test_can_create_empty_table() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, empty=True)

    # THEN
    assert relation.shape == (0, 5)
