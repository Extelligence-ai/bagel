from bagel_mcp.logging.ros2.db3 import LoggingDataset
from bagel_mcp.source.ros2.db3 import SourceFactory
from bagel_mcp.topic.ros2.db3 import TopicRegistry


def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (6, 3)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/db3_zstd/part_0.db3.zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1758921866.9605992
    )

    # THEN
    assert relation.shape == (3, 3)
