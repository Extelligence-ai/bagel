import pathlib

import pytest

from settings import settings
from src import artifacts


def test_should_return_arrow_file() -> None:
    # GIVEN
    source_uuid = "00000000-1111-2222-3333-444444444444"
    seeds = ["cat", "says", "meow"]
    prefix = "topic"

    # WHEN
    result = artifacts.arrow_file(source_uuid, seeds, prefix)

    # THEN
    assert str(result) == str(
        pathlib.Path(settings.CACHE_DIRECTORY)
        / "data"
        / "source_id=00000000-1111-2222-3333-444444444444"
        / "topic_f3aebf10.arrow"
    )


def test_should_raise_if_empty_seeds() -> None:
    # GIVEN
    source_uuid = "00000000-1111-2222-3333-444444444444"
    seeds: list[str] = []
    prefix = "topic"

    # WHEN / THEN
    with pytest.raises(ValueError, match="Seeds list must not be empty."):
        artifacts.arrow_file(source_uuid, seeds, prefix)


def test_should_return_git_clone_directory() -> None:
    # WHEN
    result = artifacts.git_clone_directory()

    # THEN
    assert str(result) == str(pathlib.Path(settings.CACHE_DIRECTORY) / "repos")
