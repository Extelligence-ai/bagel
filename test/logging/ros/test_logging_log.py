from src.logging.ros.log import LoggingDataset
from src.source.ros.log import SourceFactory
from src.topic.ros.log import TopicRegistry


def test_logging_dataset_reads_directory_without_a_bag() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN all records across the three files, sorted by time
    assert relation.shape == (12, 3)
    timestamps = [row[0] for row in relation.fetchall()]
    assert timestamps == sorted(timestamps)


def test_logging_dataset_reads_single_file() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log/rosout.log")
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, TopicRegistry())

    # THEN
    assert relation.shape == (3, 3)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log")
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, TopicRegistry(), start_seconds=1662400010.0, end_seconds=1662400021.0
    )

    # THEN talker WARN+ERROR, /move_base WARN+ERROR
    assert relation.shape == (4, 3)


def test_severity_is_queryable_with_sql() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log")
    relation = LoggingDataset().to_duckdb(factory, TopicRegistry())

    # WHEN
    errors = relation.filter("message.level = 'ERROR'").fetchall()

    # THEN one ERROR each from talker, talker-1, and /move_base
    assert len(errors) == 3
    assert {row[1] for row in errors} == {"talker", "talker-1", "/move_base"}


def test_source_factory_metadata() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ros/log")

    # WHEN
    metadata = factory.metadata

    # THEN
    assert metadata["total_message_count"] == 12
    assert metadata["start_seconds"] == 1662400000.0405089
    assert metadata["end_seconds"] == 1662400030.0
    assert len(metadata["files"]) == 3
