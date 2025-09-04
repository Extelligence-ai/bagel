"""Provide a data source for reading Betaflight Blackbox logs."""

import orangebox

from src.source import base, errors


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from Betaflight Blackbox logs."""

    def __init__(
        self,
        path: str,
        log_index: int,
        allow_invalid_header: bool = False,
    ) -> None:
        """Initialize the Betaflight Blackbox data source factory.

        Args:
            path (str): Path to the .bbl file.
            log_index (int): Index within log file. When using a built-in flash chip for logging,
                flight logs are combined into a single .bbl file. The log_index parameter specifies
                which flight log to read from the combined file.
            allow_invalid_header (bool, optional): Allow skipping of badly formatted headers.

        """
        super().__init__(path)
        self._log_index = log_index
        self._allow_invalid_header = allow_invalid_header

    def build(self) -> orangebox.Parser:
        """Return an orangebox Parser object."""
        return orangebox.Parser.load(
            path=str(self.path),
            log_index=self._log_index,
            allow_invalid_header=self._allow_invalid_header,
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the Betaflight Blackbox file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".bbl":
            return False, errors.InvalidFileExtensionError(".bbl", self.path)

        return True, None
