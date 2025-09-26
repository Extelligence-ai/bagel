from src.logging.px4.ulg import LoggingDataset
from src.source.px4.ulg import SourceFactory
from src.topic.px4.ulg import TopicRegistry


def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/px4/sample.ulg")
    registry = TopicRegistry(download_description=False)
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (5, 2)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/px4/sample.ulg")
    registry = TopicRegistry(download_description=False)
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry, start_seconds=None, end_seconds=468.49294)

    # THEN
    assert relation.shape == (2, 2)
