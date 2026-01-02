"""Provide a PyArrow dataset for reading JSON files."""

from src.di import module
from src.di.types.data_source import is_json_directory, is_json_file
from src.source.pyarrow import base


class SourceFactory(base.SourceFactory):
    """A data source factory for reading JSON files as a PyArrow dataset."""

    def __init__(  # noqa: PLR0913
        self,
        path: str,
        partitioning: str | list[str] | None = None,
        partition_base_dir: str | None = None,
        exclude_invalid_files: bool = True,
        ignore_prefixes: list[str] | None = None,
        timestamp_access_path: list[str] | None = None,
        timestamp_format: str | None = None,
    ) -> None:
        """Initialize a PyArrow JSON data source factory.

        Many of the arguments are directly passed to pyarrow.dataset.dataset().
        https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html

        Args:
            path (str): Path to the JSON file or directory.
            partitioning (str | list[str] | None, optional): The partitioning scheme specified with
                the partitioning() function. A flavor string can be used as shortcut, and with a
                list of field names a DirectoryPartitioning will be inferred.
            partition_base_dir (str | None, optional): For the purposes of applying the
                partitioning, paths will be stripped of the partition_base_dir. Files not matching
                the partition_base_dir prefix will be skipped for partitioning discovery.
                The ignored files will still be part of the Dataset, but will not have partition
                information.
            exclude_invalid_files (bool, optional): If True, invalid files will be excluded
                (file format specific check). This will incur IO for each files in a serial and
                single threaded fashion. Disabling this feature will skip the IO, but unsupported
                files may be present in the Dataset (resulting in an error at scan time).
            ignore_prefixes (list[str] | None, optional): Files matching any of these prefixes will
                be ignored by the discovery process. This is matched to the basename of a path.
                By default this is ['.', '_']. Note that discovery happens only if a directory is
                passed as source.
            timestamp_access_path (list[str] | None, optional): A list of access keys to extract
                the timestamp value from each message. If None, the current system time is used.
            timestamp_format (str | None, optional): The format string to parse the timestamp value.
                For example, it could be a time unit like, "seconds", "milliseconds",
                "microseconds", or "nanoseconds". It can also be a format string like
                "%Y-%m-%d %H:%M:%S.%f".

        """
        super().__init__(
            path=path,
            partitioning=partitioning,
            partition_base_dir=partition_base_dir,
            exclude_invalid_files=exclude_invalid_files,
            ignore_prefixes=ignore_prefixes,
            timestamp_access_path=timestamp_access_path,
            timestamp_format=timestamp_format,
        )

    def build(self) -> base.PyArrowDataset:
        """Return a PyArrowDataset object."""
        return self._build(file_format="json")

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate if the given path is a valid JSON or JSONL file/directory."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not is_json_file(self.path) and not is_json_directory(self.path):
            return False, ValueError(f"{self.path} is not a valid JSON or JSONL file/directory.")

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SourceFactory
