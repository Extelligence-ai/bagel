"""Read messages from a Betaflight .BFL file by topic."""

import pathlib

from src.reader.betaflight.bbl import topic as bbl_topic


class TopicMessageReader(bbl_topic.TopicMessageReader):
    """Read messages from a Betaflight .BFL file by topic.

    The .BFL log doesn't have a log index, so we default to 1.
    """

    def __init__(
        self,
        robolog_path: str | pathlib.Path,
        use_cache: bool = True,
    ) -> None:
        """Initialize the TopicMessageReader."""
        super().__init__(robolog_path, log_index=1, use_cache=use_cache)
