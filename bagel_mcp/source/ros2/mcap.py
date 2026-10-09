"""A data source factory for reading from ROS2 MCAP bags.

Back-compat only: ``resolve()`` routes every MCAP file through the format-agnostic
``bagel_mcp.source.mcap`` reader. This factory is reached only when a caller asks for
the ``ros2.mcap`` type explicitly; it reads rosbag2's metadata through the bag backend
and the messages through the ``mcap`` library.
"""

import pathlib

from pydantic import BaseModel, ConfigDict

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.source.ros2 import base, decompress

FEATURE = "Reading ROS 2 MCAP bags through rosbag2 metadata"


class McapRos2Bag(BaseModel):
    """Represent a data source for a ROS2 bag in MCAP format.

    It can be either a single .mcap file or a directory containing
    multiple .mcap files and metadata.yaml.

    """

    path: pathlib.Path
    metadata: bags.BagInfo

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def mcap_files(self) -> list[pathlib.Path]:
        """Return a list of all .mcap files in the bag."""
        if self.path.is_file():
            return [self.path]
        files = []
        for file in self.metadata.files:
            path = self.path / file.path
            if not path.exists() and path.with_suffix(path.suffix + ".zstd").exists():
                # Recorded with file-level zstd compression: expand it into the cache
                # for the mcap reader (the bag backend reads the directory as it is).
                path = decompress.ros2bag(path.with_suffix(path.suffix + ".zstd"))
            files.append(path)
        return files

    def __hash__(self) -> str:
        """Needed for functools caching."""
        return hash(str(self.path))


class SourceFactory(base.SourceFactory):
    """A data source factory for reading from ROS2 MCAP bags."""

    def build(self) -> McapRos2Bag:
        """Return an McapRos2Bag object."""
        return McapRos2Bag(path=self.path, metadata=self._info)


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 2)."""
    bags.require(FEATURE, ros_version=2)
    module.global_registry[__name__] = SourceFactory
