from src.logging.ardupilot.bin import LoggingDataset
from src.source.ardupilot.bin import SourceFactory
from src.topic.ardupilot.bin import TopicRegistry


def test_logging_dataset() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(factory, registry)

    # THEN
    assert relation.shape == (45, 3)


def test_can_select_time_range() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/ardupilot/sample.bin")
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN
    relation = dataset.to_duckdb(
        factory, registry, start_seconds=None, end_seconds=1754307093.2008798
    )

    # THEN
    assert relation.shape == (3, 3)
