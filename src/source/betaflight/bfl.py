"""Provide a data source for reading Betaflight Blackbox logs."""

from src import di
from src.source import errors
from src.source.betaflight import bbl


class SourceFactory(bbl.SourceFactory):
    """A data source factory for reading from Betaflight Blackbox logs."""

    def __init__(self, path: str) -> None:
        """Initialize the Betaflight Blackbox data source factory.

        Args:
            path (str): Path to the .BFL file.

        """
        # On drones with an SD card slot, each flight log is saved as a distinct .BFL file.
        super().__init__(
            path,
            log_index=1,  # Therefore, always read the first (and only) flight log.
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the Betaflight Blackbox file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".BFL":
            return False, errors.InvalidFileExtensionError(".BFL", self.path)

        return True, None


def register() -> None:
    """Register module for dependency injection."""
    di.module_registry[__name__] = SourceFactory
