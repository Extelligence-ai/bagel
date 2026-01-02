from src.message.ros1.bag import MessageDataset
from src.source.ros1.bag import SourceFactory
from src.topic.ros1.bag import TopicRegistry


def test_message_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros1/sample.bag")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.to_df().shape == (34, 3)


def test_can_select_topic() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros1/sample.bag")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["/turtle1/cmd_vel"])

    # THEN
    assert relation.to_df().shape == (33, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros1/sample.bag")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1660676079.0116584
    )

    # THEN
    assert relation.to_df().shape == (2, 3)


def test_can_create_empty_table() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros1/sample.bag")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, empty=True)

    # THEN
    assert relation.to_df().shape == (0, 3)
