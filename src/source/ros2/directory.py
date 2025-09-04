"""Provide a data source for reading ROS2 bag directories."""

import rosbag2_py

from src.source import base, errors


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from ROS2 bag directories."""

    def __init__(
        self,
        path: str,
        storage_id: str = "",
    ) -> None:
        """Initialize the ROS2 Bag data source factory.

        Args:
            path (str): Path to the ROS2 bag directory.
            storage_id (str, optional): The storage backend id, e.g., 'sqlite3', 'mcap' or ''.

        """
        if storage_id not in {"", "sqlite3", "mcap"}:
            raise ValueError(f"Unsupported storage_id: {storage_id}")
        self._storage_id = storage_id
        self._metadata = rosbag2_py.Info().read_metadata(path, self._storage_id)
        super().__init__(path)

    def build(self) -> rosbag2_py.SequentialReader:
        """Return a ROS2 SequentialReader or SequentialCompressionReader object."""
        storage_options = rosbag2_py.StorageOptions(
            uri=str(self.path),
            storage_id=self._storage_id,
        )
        converter_options = rosbag2_py.ConverterOptions(
            input_serialization_format="",
            output_serialization_format="",
        )
        reader = (
            rosbag2_py.SequentialReader()
            if self._metadata.compression_format == ""
            else rosbag2_py.SequentialCompressionReader()
        )
        reader.open(storage_options, converter_options)
        return reader

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS2 bag directory path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_dir():
            return False, errors.PathNotDirectoryError(self.path)

        if not (self.path / "metadata.yaml").exists():
            return False, errors.MissingFilesError([str(self.path / "metadata.yaml")])

        missing_chunks = [
            f for f in self._metadata.relative_file_paths if not (self.path / f).exists()
        ]

        if missing_chunks:
            return False, errors.MissingFilesError(missing_chunks)

        return True, None
