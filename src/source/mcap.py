"""A data source factory for MCAP files, independent of any robotics middleware.

Reads everything from the MCAP container itself (header, summary, statistics) --
no rosbag2 metadata.yaml or ROS installation required. Handles a single ``.mcap``
file or a directory of ``.mcap`` files (including rosbag2-produced MCAP bags,
whose ``metadata.yaml`` is simply ignored). Zstandard-compressed files
(``.mcap.zstd``) are decompressed transparently into the cache directory,
leaving the source untouched.
"""

import functools
import pathlib
import uuid
from typing import Any

import zstandard
from mcap.reader import make_reader
from mcap.summary import Summary
from pydantic import BaseModel

from settings import settings
from src.di import module
from src.source import base

NANOSECOND = 1
SECOND = 1_000_000_000 * NANOSECOND

MCAP_MAGIC = b"\x89MCAP0\r\n"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def decompress(file: pathlib.Path) -> pathlib.Path:
    """Decompress a ``.mcap.zstd`` file into the cache and return the decompressed path.

    The cache key includes the source path, size, and mtime, so a changed source is
    re-decompressed while repeat reads are free. The source file is never modified.
    """
    stat = file.stat()
    key = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{file.resolve()}_{stat.st_size}_{stat.st_mtime_ns}"))
    output = pathlib.Path(settings.CACHE_DIRECTORY) / "decompressed" / key / file.stem
    if output.exists():
        return output

    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_suffix(".partial")
    with open(file, "rb") as f_in, open(partial, "wb") as f_out:
        zstandard.ZstdDecompressor().copy_stream(f_in, f_out)
    partial.rename(output)  # atomic-ish: readers only ever see complete files
    return output


class McapBag(BaseModel):
    """Represent an MCAP data source: a single .mcap file or a directory of them."""

    path: pathlib.Path

    @property
    def mcap_files(self) -> list[pathlib.Path]:
        """Return all .mcap files in the data source, decompressing .mcap.zstd as needed."""
        if self.path.is_file():
            candidates = [self.path]
        else:
            candidates = sorted([*self.path.glob("*.mcap"), *self.path.glob("*.mcap.zstd")])
        return [
            decompress(file) if file.name.endswith(".mcap.zstd") else file for file in candidates
        ]

    def __hash__(self) -> int:
        """Needed for functools caching."""
        return hash(str(self.path))


@functools.lru_cache
def summaries(bag: McapBag) -> list[Summary]:
    """Return the MCAP summary section of every file in the bag."""
    result = []
    for file in bag.mcap_files:
        with open(file, "rb") as stream:
            summary = make_reader(stream).get_summary()
            if summary is not None:
                result.append(summary)
    return result


class SourceFactory(base.BoundedSourceFactory, base.FileBasedSourceFactory):
    """A data source factory for MCAP files."""

    def build(self) -> McapBag:
        """Return an McapBag object."""
        return McapBag(path=self.path)

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate that the path is an MCAP file or a directory containing them."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        files = McapBag(path=self.path).mcap_files
        if not files:
            return False, ValueError(f"No .mcap files found at {self.path}")

        for file in files:
            with open(file, "rb") as stream:
                if stream.read(len(MCAP_MAGIC)) != MCAP_MAGIC:
                    return False, ValueError(f"{file} is not a valid MCAP file")

        return True, None

    @property
    def _statistics(self) -> list:
        return [s.statistics for s in summaries(self.build()) if s.statistics is not None]

    @property
    def total_message_count(self) -> int:
        """Return the total number of messages."""
        return sum(statistics.message_count for statistics in self._statistics)

    @property
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""
        return min(statistics.message_start_time for statistics in self._statistics) / SECOND

    @property
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""
        return max(statistics.message_end_time for statistics in self._statistics) / SECOND

    @property
    def profiles(self) -> list[str]:
        """Return the MCAP profiles of the files (e.g. 'ros2', 'ros1', '' for generic)."""
        profiles = set()
        for file in self.build().mcap_files:
            with open(file, "rb") as stream:
                profiles.add(make_reader(stream).get_header().profile)
        return sorted(profiles)

    @property
    def topic_information(self) -> list[dict[str, Any]]:
        """Return per-topic type names, message encodings, and message counts."""
        topics: dict[str, dict[str, Any]] = {}
        for summary in summaries(self.build()):
            counts = summary.statistics.channel_message_counts if summary.statistics else {}
            for channel_id, channel in summary.channels.items():
                schema = summary.schemas.get(channel.schema_id)
                info = topics.setdefault(
                    channel.topic,
                    {
                        "name": channel.topic,
                        "type": schema.name if schema else channel.message_encoding,
                        "message_encoding": channel.message_encoding,
                        "schema_encoding": schema.encoding if schema else None,
                        "message_count": 0,
                    },
                )
                info["message_count"] += counts.get(channel_id, 0)
        return sorted(topics.values(), key=lambda info: info["name"])

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the MCAP data source."""
        return {
            **self._bounded_metadata,
            **self._file_based_metadata,
            "profiles": self.profiles,
            "relative_file_paths": [str(f.name) for f in self.build().mcap_files],
            "topic_information": self.topic_information,
        }


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
