"""End-to-end tests for zstd-compressed MCAP through the generic (ROS-free) path.

Uses the bundled `data/sample/ros2/mcap_zstd` bag: a rosbag2-produced directory whose
single part is `part_0.mcap.zstd`. Decompression goes to the cache directory, keyed by
source path/size/mtime, and never touches the source.
"""

import pathlib

import pytest

from bagel import server
from bagel.di import module
from bagel.settings import settings
from bagel.source import mcap as mcap_source

SAMPLE_DIR = "./data/sample/ros2/mcap_zstd"
SAMPLE_FILE = "./data/sample/ros2/mcap_zstd/part_0.mcap.zstd"


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path / "cache"))


def test_zstd_bag_reads_through_generic_path() -> None:
    factory = module.provide("bagel.source.mcap", {"path": SAMPLE_DIR})
    registry = module.provide("bagel.topic.mcap", {})
    bag = factory.build()

    topics = registry.available_topics(bag)
    assert "/rosout" in topics
    # ros2msg schema parsed from the decompressed file, no ROS involved.
    struct = registry.struct("/rosout", bag)
    assert "msg" in struct.names

    rows = server.query_messages(
        path=SAMPLE_DIR,
        sql_statement='SELECT COUNT(*) AS n FROM "/rosout"',
        topic="/rosout",
    )
    assert rows[0]["n"] > 0


def test_bare_zstd_file_reads_too() -> None:
    factory = module.provide("bagel.source.mcap", {"path": SAMPLE_FILE})
    registry = module.provide("bagel.topic.mcap", {})
    assert "/rosout" in registry.available_topics(factory.build())


def test_decompression_is_cached_and_source_untouched() -> None:
    source = pathlib.Path(SAMPLE_FILE)
    before = source.stat().st_mtime_ns

    first = mcap_source.decompress(source)
    assert first.exists()
    assert first.name == "part_0.mcap"
    first_mtime = first.stat().st_mtime_ns

    second = mcap_source.decompress(source)
    assert second == first
    assert second.stat().st_mtime_ns == first_mtime, "second call must reuse the cache"
    assert source.stat().st_mtime_ns == before, "source file must not be modified"
    # Nothing new was written next to the source (the old ROS path used to do this).
    assert not (source.parent / "part_0.mcap").exists()
