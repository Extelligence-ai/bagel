"""Provide a data source for reading ROS1 bags."""

import rosbag

from src.source import base, errors


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from ROS1 bags."""

    def __init__(
        self,
        path: str,
        allow_unindexed: bool = True,
    ) -> None:
        """Initialize the ROS1 Bag data source factory.

        Args:
            path (str): Path to the .bag file.
            allow_unindexed (bool, optional): If True, allow opening unindexed bags.

        """
        super().__init__(path)
        self._allow_unindexed = allow_unindexed

    def build(self) -> rosbag.Bag:
        """Return a ROS1 Bag object."""
        return rosbag.Bag(
            f=str(self.path),
            mode="r",
            allow_unindexed=self._allow_unindexed,
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS1 bag file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".bag":
            return False, errors.InvalidFileExtensionError(".bag", self.path)

        return True, None
