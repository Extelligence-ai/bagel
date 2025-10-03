"""An abstract base class for timeseries data source factories."""

import abc
import hashlib
import pathlib
import uuid
from typing import Any, Final

BYTE = 1
KB = 1024 * BYTE
MB = 1024 * KB

MD5_READ_SIZE: Final = 64 * MB


class SourceFactory(abc.ABC):
    """An abstract base class for timeseries data source factories.

    This ABC provides a common contract for implementing timeseries data source factories.
    A timeseries data source could be a static file, a database, or a real-time stream.

    Note:
        Constructors of all subclasses must only accept primitive types (e.g.,
        str, int, bool). This ensures instances can be reliably serialized and
        recreated via dependency injection.

    """

    @property
    @abc.abstractmethod
    def uuid(self) -> str:
        """A unique identifier for the data source."""

    @property
    @abc.abstractmethod
    def metadata(self) -> dict[str, Any]:
        """Return metadata about the data source."""

    @abc.abstractmethod
    def build(self) -> object:
        """Return a new instance of the data source object.

        This method could return a log reader, a database connection, or a streaming API client.
        """


class BoundedSourceFactory(SourceFactory):
    """A base class for factories of bounded data source.

    A bounded data source has a defined start and end time.

    """

    @property
    def _bounded_metadata(self) -> dict[str, Any]:
        """Return metadata about the data source."""
        return {
            "total_message_count": self.total_message_count,
            "start_seconds": self.start_seconds,
            "end_seconds": self.end_seconds,
            "duration_seconds": self.duration_seconds,
        }

    @property
    @abc.abstractmethod
    def total_message_count(self) -> int:
        """Return the total number of messages."""

    @property
    @abc.abstractmethod
    def start_seconds(self) -> float:
        """Return the start timestamp in seconds."""

    @property
    @abc.abstractmethod
    def end_seconds(self) -> float:
        """Return the end timestamp in seconds."""

    @property
    def duration_seconds(self) -> float:
        """Return the duration in seconds."""
        return self.end_seconds - self.start_seconds


class FileBasedSourceFactory(SourceFactory):
    """A base class for factories of data source from local file system."""

    def __init__(self, path: str) -> None:
        """Initialize the local file system data source factory.

        Args:
            path (str): The path to the local file or directory.

        """
        self._path = pathlib.Path(path)
        is_valid, error = self.validate_path()
        if not is_valid:
            raise error

    @property
    def uuid(self) -> str:
        """A unique identifier generated from the content of the local file or directory."""
        files = (
            [self.path]
            if self.path.is_file()
            else [f for f in sorted(self.path.glob("**/*")) if f.is_file()]
        )
        hashes = [self._md5_hash(f) for f in files]
        return str(uuid.uuid5(uuid.NAMESPACE_OID, "_".join(hashes)))

    @property
    def _file_based_metadata(self) -> dict[str, Any]:
        """Return metadata about the data source."""
        return {
            "path": str(self.path),
            "size_bytes": self.size_bytes,
        }

    @property
    def path(self) -> pathlib.Path:
        """The path to the local file or directory."""
        return self._path

    @property
    def size_bytes(self) -> int:
        """The total size of the local file or directory in bytes."""
        if self.path.is_file():
            return self.path.stat().st_size
        return sum(f.stat().st_size for f in self.path.glob("**/*") if f.is_file())

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the path to data source on the local file system.

        Returns:
            tuple[bool, Exception | None]: A tuple containing a boolean indicating
                whether the path is valid and an optional exception.

        """
        raise NotImplementedError

    def _md5_hash(self, file_path: pathlib.Path) -> str:
        """Calculate the MD5 hash of a file."""
        hash_func = hashlib.new("md5")  # noqa: S324
        with open(file_path, "rb") as f:
            hash_func.update(f.read(MD5_READ_SIZE))
        return hash_func.hexdigest()
