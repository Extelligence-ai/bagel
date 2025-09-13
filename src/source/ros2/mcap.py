"""A data source factory for reading from ROS2 MCAP bags."""

import pathlib

import rosbag2_py
from pydantic import BaseModel

from src.source import base, errors
from src.source.ros2 import decompress


class McapRos2Bag(BaseModel):
    """Represent a data source for a ROS2 bag in MCAP format.

    It can be either a single .mcap file or a directory containing
    multiple .mcap files and metadata.yaml.

    """

    path: pathlib.Path

    @property
    def metadata(self) -> rosbag2_py.BagMetadata:
        """Return the metadata of the ROS2 bag."""
        return rosbag2_py.Info().read_metadata(str(self.path), "")

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


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from ROS2 MCAP bags."""

    def __init__(self, path: str) -> None:
        """Initialize the ROS2 MCAP bag data source factory.

        Args:
            path (str): The path to the ROS2 MCAP bag file or directory.

        """
        decompressed_path = str(decompress.ros2bag(pathlib.Path(path)))
        super().__init__(decompressed_path)

    def build(self) -> McapRos2Bag:
        """Return an McapRos2Bag object."""
        return McapRos2Bag(path=self.path)

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS2 bag path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        missing_files = [f for f in self.build().mcap_files if not f.exists()]
        if missing_files:
            return False, errors.MissingFilesError([str(f) for f in missing_files])

        return True, None
