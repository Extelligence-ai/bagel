from src.message.ardupilot.bin import MessageDataset
from src.source.ardupilot.bin import SourceFactory
from src.topic.ardupilot.bin import TopicRegistry


def test_message_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (69769, 66)


def test_can_select_topic() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["AETR"])

    # THEN
    assert relation.shape == (1028, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1754307092.581066
    )

    # THEN
    assert relation.shape == (8, 66)


def test_can_create_empty_table() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, empty=True)

    # THEN
    assert relation.to_df().shape == (0, 66)
