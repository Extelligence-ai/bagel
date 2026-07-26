"""Bad-argument tests for MCP tool functions: errors must be graceful."""

import pytest

import server

# Paths that should raise errors
BAD_PATHS_SHOULD_RAISE = ["/does/not/exist.mcap", "relative/nope.bag", "\x00null"]


@pytest.mark.parametrize("path", BAD_PATHS_SHOULD_RAISE)
def test_describe_data_source_bad_path(path: str) -> None:
    """Bad paths should raise controlled errors, not crash."""
    with pytest.raises((FileNotFoundError, ValueError, OSError)):
        server.describe_data_source(path)


def test_describe_data_source_empty_path_raises() -> None:
    """Empty string path must raise, not silently resolve to the current dir."""
    with pytest.raises(ValueError):
        server.describe_data_source("")


def test_query_messages_bad_sql() -> None:
    # Use a real sample so the failure is in SQL handling, not path resolution.
    import pathlib
    sample = pathlib.Path("data/sample/ros2/mcap")
    if not sample.exists():
        pytest.skip("ros2 sample not present")
    with pytest.raises(Exception):  # noqa: B017 -- documenting current surface
        server.query_messages(str(sample), "DROP TABLE x; -- not a select", "/nope")


def test_read_loggings_inverted_window() -> None:
    import pathlib
    sample = pathlib.Path("data/sample/ros2/mcap")
    if not sample.exists():
        pytest.skip("ros2 sample not present")
    # end before start: must not hang or return garbage.
    result = server.read_loggings(str(sample), start_seconds=100.0, end_seconds=1.0)
    assert isinstance(result, list)
