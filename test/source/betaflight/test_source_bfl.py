import pytest

from src.source.betaflight import bfl


def test_source_factory() -> None:
    # GIVEN
    factory = bfl.SourceFactory("data/sample/betaflight/sample.BFL")

    # WHEN / THEN
    assert factory.total_message_count == 184383
    assert factory.start_seconds == 0.0
    assert factory.end_seconds == 345.601958


def test_validate_path_should_raise() -> None:
    # WHEN / THEN
    with pytest.raises(FileNotFoundError):
        bfl.SourceFactory("data/sample/betaflight/non_exist.BFL")
