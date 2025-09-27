"""Common errors raised by the source module."""

import pathlib


class PathNotFileError(Exception):
    """Raised when a path is not a file."""


class PathNotDirectoryError(Exception):
    """Raised when a path is not a directory."""


class MissingFilesError(Exception):
    """Raised when required files are missing."""


class InvalidFileExtensionError(Exception):
    """Raised when a file has an invalid extension."""

    def __init__(self, extension: str, path: pathlib.Path) -> None:
        """Initialize the error."""
        super().__init__(f"Expected a file with extension {extension} but got: {path}")
