"""A data source factory for reading from ROS2 MCAP bags."""

import pathlib

import rosbag2_py
from pydantic import BaseModel, ConfigDict

from src.di import module
from src.source.ros2 import base


class McapRos2Bag(BaseModel):
    """Represent a data source for a ROS2 bag in MCAP format.

    It can be either a single .mcap file or a directory containing
    multiple .mcap files and metadata.yaml.

    """

    path: pathlib.Path
    metadata: rosbag2_py.BagMetadata

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
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
