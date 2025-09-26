from src.logging.ros1.bag import LoggingDataset
from src.source.ros1.bag import SourceFactory
from src.topic.ros1.bag import TopicRegistry


def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros1/sample.bag")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (1, 3)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros1/sample.bag")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1660676075.8220897
    )

    # THEN
    assert relation.shape == (0, 3)
