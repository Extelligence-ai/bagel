import os
import pathlib
import time

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


@pytest.fixture
def fake_cache(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> list[pathlib.Path]:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
    source_dir = tmp_path / "data" / "source_id=abc"
    source_dir.mkdir(parents=True)
    files = []
    for index in range(4):
        file = source_dir / f"topics_{index:08d}.arrow"
        file.write_bytes(b"x" * 1000)
        stamp = 1_700_000_000 + index * 1000  # file 0 oldest, file 3 newest
        os.utime(file, (stamp, stamp))
        files.append(file)
    (tmp_path / "data" / "sink=deadbeef").mkdir(parents=True)
    (tmp_path / "data" / "sink=deadbeef" / "current.jsonl").write_bytes(b"y" * 5000)
    return files


def test_evict_noop_under_cap(
    fake_cache: list[pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 10_000)
    assert artifacts.evict_arrow_cache() == 0
    assert all(file.exists() for file in fake_cache)


def test_evict_deletes_oldest_first_until_under_cap(
    fake_cache: list[pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 2_500)
    assert artifacts.evict_arrow_cache() == 2
    assert not fake_cache[0].exists() and not fake_cache[1].exists()
    assert fake_cache[2].exists() and fake_cache[3].exists()


def test_evict_zero_disables(
    fake_cache: list[pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 0)
    assert artifacts.evict_arrow_cache() == 0


def test_evict_never_touches_sink_trees(
    fake_cache: list[pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 1)
    artifacts.evict_arrow_cache()
    sink_file = pathlib.Path(settings.CACHE_DIRECTORY) / "data" / "sink=deadbeef" / "current.jsonl"
    assert sink_file.exists()


def test_fresh_utime_survives_eviction(
    fake_cache: list[pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """#134: a hot (just-read) file is newest and outlives older entries."""
    monkeypatch.setattr(settings, "CACHE_MAX_BYTES", 1_500)
    now = time.time()
    os.utime(fake_cache[0], (now, now))  # oldest file becomes hot
    artifacts.evict_arrow_cache()
    assert fake_cache[0].exists()
    assert not fake_cache[1].exists()


def test_to_duckdb_rebuilds_after_cache_file_deleted(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#134: losing a cache file (eviction race) degrades to a miss, not an error.

    Note: `src.di.types.data_source.resolve()` returns a `DataSource` enum member,
    not a bundle of factory/registry/dataset objects, so this mirrors the direct
    construction pattern used by e.g. `test/message/px4/test_message_ulg.py`
    (and the timestamp args from `test/pipeline/test_preview_pipeline.py`) rather
    than calling `resolve()` directly.
    """
    from src.message.pyarrow.csv import MessageDataset
    from src.source.pyarrow.csv import SourceFactory
    from src.topic.pyarrow.csv import TopicRegistry

    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
    factory = SourceFactory(
        "data/sample/pyarrow/csv/flight.csv", timestamp_column="t", timestamp_format="seconds"
    )
    registry = TopicRegistry()
    dataset = MessageDataset()

    relation = dataset.to_duckdb(factory, registry)
    rows = relation.df().shape[0]
    cached = list((tmp_path / "data").glob("source_id=*/**/*.arrow"))
    assert len(cached) == 1
    cached[0].unlink()
    relation_again = dataset.to_duckdb(factory, registry)
    assert relation_again.df().shape[0] == rows


def test_directory_size_bytes(tmp_path: pathlib.Path) -> None:
    assert artifacts.directory_size_bytes(tmp_path / "missing") == 0
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.bin").write_bytes(b"x" * 100)
    (tmp_path / "sub" / "b.bin").write_bytes(b"y" * 50)
    assert artifacts.directory_size_bytes(tmp_path) == 150
