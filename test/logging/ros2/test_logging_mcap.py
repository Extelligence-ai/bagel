from bagel_mcp.logging.ros2.mcap import LoggingDataset
from bagel_mcp.source.ros2.mcap import SourceFactory
from bagel_mcp.topic.ros2.mcap import TopicRegistry


def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap_zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.to_df().shape == (6, 3)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros2/mcap_zstd")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1758921828.5230587
    )

    # THEN
    assert relation.to_df().shape == (3, 3)
