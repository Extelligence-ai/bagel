import pytest

from bagel_mcp.logging.betaflight.bbl import LoggingDataset, LoggingMessagesNotSupportedError
from bagel_mcp.source.betaflight.bbl import SourceFactory
from bagel_mcp.topic.betaflight.bbl import TopicRegistry


def test_should_raise() -> None:
    # GIVEN
    factory = SourceFactory("data/sample/betaflight/sample.bbl", log_index=1)
    registry = TopicRegistry()
    dataset = LoggingDataset()

    # WHEN / THEN
    with pytest.raises(LoggingMessagesNotSupportedError):
        dataset.to_duckdb(factory, registry)
