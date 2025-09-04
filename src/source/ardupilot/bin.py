"""Provide a data source for reading ArduPilot Dataflash logs."""

from pymavlink import DFReader

from src.source import base, errors


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from ArduPilot Dataflash logs."""

    def __init__(
        self,
        path: str,
        zero_time_base: bool = False,
    ) -> None:
        """Initialize the ArduPilot Dataflash data source factory.

        Args:
            path (str): Path to the .bin file.
            zero_time_base (bool, optional): If True, timestamps start from zero instead of using
                GPS time. False does not guarantee epoch time if no valid GPS messages.

        """
        super().__init__(path)
        self._zero_time_base = zero_time_base

    def build(self) -> DFReader.DFReader_binary:
        """Return an ArduPilot DFReader_binary object."""
        return DFReader.DFReader_binary(
            filename=str(self.path),
            zero_time_base=self._zero_time_base,
            progress_callback=None,
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the ArduPilot Dataflash file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".bin":
            return False, errors.InvalidFileExtensionError(".bin", self.path)

        return True, None
