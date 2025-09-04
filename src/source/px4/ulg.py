"""Provide a data source for reading PX4 ULogs."""

from pyulog import core

from src.source import base, errors


class SourceFactory(base.LocalFileSystemSourceFactory):
    """A data source factory for reading from PX4 ULogs."""

    def __init__(
        self,
        path: str,
        message_name_filter_list: list[str] | None = None,
        disable_str_exceptions: bool = True,
        parse_header_only: bool = False,
    ) -> None:
        """Initialize the PX4 ULog data source factory.

        Args:
            path (str): Path to the .ulg file.
            message_name_filter_list (list[str] | None, optional): A list of message names to load.
                If None, load everything.
            disable_str_exceptions (bool, optional): If True, ignore string parsing errors.
            parse_header_only (bool, optional): If True, only parse the header.

        """
        super().__init__(path)
        self._message_name_filter_list = message_name_filter_list
        self._disable_str_exceptions = disable_str_exceptions
        self._parse_header_only = parse_header_only

    def build(self) -> core.ULog:
        """Return a PX4 ULog object."""
        return core.ULog(
            log_file=str(self.path),
            message_name_filter_list=self._message_name_filter_list,
            disable_str_exceptions=self._disable_str_exceptions,
            parse_header_only=self._parse_header_only,
        )

    def validate_path(self) -> tuple[bool, Exception | None]:
        """Validate the PX4 ULog file path."""
        if not self.path.exists():
            return False, FileNotFoundError(self.path)

        if not self.path.is_file():
            return False, errors.PathNotFileError(self.path)

        if self.path.suffix != ".ulg":
            return False, errors.InvalidFileExtensionError(".ulg", self.path)

        return True, None
