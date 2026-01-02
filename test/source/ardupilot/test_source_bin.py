import pytest

from src.source.ardupilot import bin as ardubin


def test_source_factory() -> None:
    # GIVEN
    factory = ardubin.SourceFactory("data/sample/ardupilot/sample.bin")

    # WHEN / THEN
    assert factory.total_message_count == 69769
    assert factory.start_seconds == 1754307092.5810509
    assert factory.end_seconds == 1754307201.7701719


def test_validate_path_should_raise() -> None:
    # WHEN / THEN
    with pytest.raises(FileNotFoundError):
        ardubin.SourceFactory("data/sample/ardupilot/non_exist.bin")
