"""A data source factory for reading from ROS2 MCAP bags.

Back-compat only: ``resolve()`` routes every MCAP file through the format-agnostic
``bagel_mcp.source.mcap`` reader, which needs no ROS. This factory is reached only
when a caller asks for the ``ros2.mcap`` type explicitly.
"""

import pathlib
from typing import Any

from pydantic import BaseModel, ConfigDict

from bagel_mcp import ros_native
from bagel_mcp.di import module
from bagel_mcp.source.ros2 import base

FEATURE = "Reading ROS 2 MCAP bags through rosbag2"


class McapRos2Bag(BaseModel):
    """Represent a data source for a ROS2 bag in MCAP format.

    It can be either a single .mcap file or a directory containing
    multiple .mcap files and metadata.yaml.

    """

    path: pathlib.Path
    # A rosbag2_py.BagMetadata; pydantic evaluates this annotation at import time,
    # so it stays loose to keep the module importable without native ROS.
    metadata: Any

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def mcap_files(self) -> list[pathlib.Path]:
        """Return a list of all .mcap files in the bag."""
        if self.path.is_file():
            return [self.path]
        else:
            return [self.path / file.path for file in self.metadata.files]

    def __hash__(self) -> str:
        """Needed for functools caching."""
        return hash(str(self.path))


class SourceFactory(base.SourceFactory):
    """A data source factory for reading from ROS2 MCAP bags."""

    def build(self) -> McapRos2Bag:
        """Return an McapRos2Bag object."""
        return McapRos2Bag(path=self.path, metadata=self._metadata)


def register() -> None:
    """Register module for dependency injection (only where native ROS 2 is present)."""
    ros_native.require("rosbag2_py", feature=FEATURE)
    module.global_registry[__name__] = SourceFactory
