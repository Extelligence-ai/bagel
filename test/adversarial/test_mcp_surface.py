"""Bad-argument tests for MCP tool functions: errors must be graceful."""

import pytest

import server

# Paths that should raise errors
BAD_PATHS_SHOULD_RAISE = ["/does/not/exist.mcap", "relative/nope.bag", "\x00null"]

# Empty string returns successfully (characterization finding: treats "" as "." current dir)
PATHS_RETURN_SUCCESS = [""]


@pytest.mark.parametrize("path", BAD_PATHS_SHOULD_RAISE)
def test_describe_data_source_bad_path(path: str) -> None:
    """Bad paths should raise controlled errors, not crash."""
    with pytest.raises((FileNotFoundError, ValueError, OSError)):
        server.describe_data_source(path)


@pytest.mark.parametrize("path", PATHS_RETURN_SUCCESS)
def test_describe_data_source_empty_path_returns_success(path: str) -> None:
    """Empty string path returns successfully (treats as current dir fallback)."""
    result = server.describe_data_source(path)
    assert isinstance(result, list)


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
