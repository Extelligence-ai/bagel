"""Provide a data source for reading ROS2 bag files."""

import logging
import pathlib

import rosbag2_py
import zstandard as zstd

from src.source import base, errors


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from ROS2 bag files."""

    def __init__(
        self,
        path: str,
    ) -> None:
        """Initialize the ROS2 Bag data source factory.

        Args:
            path (str): Path to the ROS2 bag file.

        """
        file = pathlib.Path(path)
        self._file_ext = pathlib.Path(file.stem).suffix if file.suffix == ".zstd" else file.suffix
        self._storage_id = "mcap" if self._file_ext == ".mcap" else "sqlite3"
        super().__init__(path)

    def build(self) -> rosbag2_py.SequentialReader:
        """Return a ROS2 SequentialReader object."""
        file = self.path
        if file.suffix == ".zstd":
            decompressed_file = file.parent / file.stem
            with open(file, "rb") as f_in, open(decompressed_file, "wb") as f_out:
                zstd.ZstdDecompressor().copy_stream(f_in, f_out)
                file = decompressed_file
            logging.info("Decompressed %s and using %s instead.", self.path, file)

        storage_options = rosbag2_py.StorageOptions(
            uri=str(file),
            storage_id=self._storage_id,
        )
        converter_options = rosbag2_py.ConverterOptions(
            input_serialization_format="",
            output_serialization_format="",
        )
        reader = rosbag2_py.SequentialReader()
        reader.open(storage_options, converter_options)
        return reader

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ROS2 bag file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self._file_ext not in {".mcap", ".db3"}:
            return False, errors.InvalidFileExtensionError(".mcap or .db3", self.path)

        return True, None
