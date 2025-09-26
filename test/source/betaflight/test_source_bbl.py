import pytest

from src.source.betaflight import bbl


def test_source_factory() -> None:
    # GIVEN
    factory = bbl.SourceFactory("data/sample/betaflight/sample.bbl", log_index=1)

    # WHEN / THEN
    assert factory.total_message_count == 207737
    assert factory.start_seconds == 0.0
    assert factory.end_seconds == 126.254955


def test_validate_path_should_raise() -> None:
    # WHEN / THEN
    with pytest.raises(FileNotFoundError):
        bbl.SourceFactory("data/sample/betaflight/non_exist.bbl", log_index=1)
