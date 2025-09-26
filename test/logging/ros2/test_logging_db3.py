from src.logging.ros2.db3 import LoggingDataset
from src.source.ros2.db3 import SourceFactory
from src.topic.ros2.db3 import TopicRegistry


def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (8, 3)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1756962595.7265782
    )

    # THEN
    assert relation.shape == (2, 3)
