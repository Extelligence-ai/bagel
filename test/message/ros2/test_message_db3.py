from src.message.ros2.db3 import MessageDataset
from src.source.ros2.db3 import SourceFactory
from src.topic.ros2.db3 import TopicRegistry


def test_message_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (6074, 9)


def test_can_select_topic() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["AAA"])

    # THEN
    assert relation.shape == (804, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, start_seconds=1e-06, end_seconds=2e-06)

    # THEN
    assert relation.shape == (3010, 9)


def test_can_create_empty_table() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, empty=True)

    # THEN
    assert relation.shape == (0, 9)
