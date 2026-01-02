from src.message.betaflight.bbl import MessageDataset
from src.source.betaflight.bbl import SourceFactory
from src.topic.betaflight.bbl import TopicRegistry


def test_message_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/betaflight/sample.bbl", log_index=1)
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (207737, 3)


def test_can_select_topic() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/betaflight/sample.bbl", log_index=1)
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["I"])

    # THEN
    assert relation.shape == (3246, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/betaflight/sample.bbl", log_index=1)
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, start_seconds=None, end_seconds=22.412492)

    # THEN
    assert relation.shape == (3, 3)


def test_can_create_empty_table() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/betaflight/sample.bbl", log_index=1)
    registry = TopicRegistry()
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, empty=True)

    # THEN
    assert relation.to_df().shape == (0, 3)
