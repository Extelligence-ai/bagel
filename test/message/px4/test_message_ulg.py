from src.message.px4.ulg import MessageDataset
from src.source.px4.ulg import SourceFactory
from src.topic.px4.ulg import TopicRegistry


def test_message_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/px4/sample.ulg")
    registry = TopicRegistry(download_description=False)
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (23735, 78)


def test_can_select_topic() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/px4/sample.ulg")
    registry = TopicRegistry(download_description=False)
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, topics=["vehicle_imu_0"])

    # THEN
    assert relation.shape == (83, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/px4/sample.ulg")
    registry = TopicRegistry(download_description=False)
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, start_seconds=None, end_seconds=470.0)

    # THEN
    assert relation.shape == (895, 78)


def test_can_create_empty_table() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/px4/sample.ulg")
    registry = TopicRegistry(download_description=False)
    dataset = MessageDataset(use_cache=True)

    # WHEN
    relation = dataset.to_duckdb(factory, registry, empty=True)

    # THEN
    assert relation.to_df().shape == (0, 78)
