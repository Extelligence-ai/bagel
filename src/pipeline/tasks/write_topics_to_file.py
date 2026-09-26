"""Write messages from specified topics to a file in various formats."""

import logging
import pathlib
from enum import Enum

from settings import settings
from src.di import module
from src.pipeline import base, flatten, messages


class OutputFormat(Enum):
    """Supported output file formats."""

    CSV = "csv"
    PARQUET = "parquet"


class WriteTopicsToFile(base.ArtifactMixin, messages.TopicMessageMixin, base.Task):
    """Write messages from specified topics to a file in various formats."""

    def __init__(
        self,
        topics: list[str] | None,
        output_format: str,
        ffill: bool = False,
        flatten: bool = False,
        post_seconds: float = 0.0,
    ) -> None:
        """Initialize the task.

        Args:
            topics (list[str] | None): A list of topics to write to a file. If None, all available
                topics will be written.
            output_format (str): The format of the output files (e.g., "csv", "parquet").
            ffill (bool): Whether to apply forward-fill to the topic messages.
            flatten (bool): If True, expand each topic's struct into one scalar column per
                signal, named `topic/field/subfield` -- the shape plotting tools expect
                (e.g. PlotJuggler's CSV/Parquet loaders). Non-scalar fields (lists, maps)
                are skipped. Defaults to False (structs preserved).
            post_seconds (float): Seconds *after* `asof_seconds` to also write. With a
                time-based `lookback` this keeps `[asof - lookback, asof + post_seconds]`,
                e.g. the window around a live `on_event` whose `forward` covers the post
                window. Defaults to 0.0. Ignored for frame-based lookbacks.

        Raises:
            ValueError: If the topics list is empty when specified, or `post_seconds`
                is negative.

        """
        if topics is not None and len(topics) == 0:
            raise ValueError("If 'topics' is specified, it must contain at least one topic name.")
        self._topics = topics
        self._output_format = OutputFormat(output_format)
        self._ffill = ffill
        self._flatten = flatten
        if post_seconds < 0:
            raise ValueError(f"'post_seconds' must be non-negative, got {post_seconds}.")
        self._post_seconds = post_seconds

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""
        topics = self._topics or self.registry.available_topics(self.factory.build())
        if self._post_seconds and not (lookback and lookback.unit == base.Unit.FRAME):
            # Read up to asof + post, then trim the start back to asof - lookback.
            relation = self.to_duckdb(
                topics=topics,
                asof_seconds=asof_seconds + self._post_seconds,
                lookback=None,
                ffill=self._ffill,
            )
            if lookback is not None:
                start_seconds = max(0.0, asof_seconds - lookback.to_seconds())
                relation = relation.filter(
                    f"{settings.TIMESTAMP_SECONDS_COLUMN_NAME} >= {start_seconds}"
                )
        else:
            relation = self.to_duckdb(
                topics=topics, asof_seconds=asof_seconds, lookback=lookback, ffill=self._ffill
            )
        if self._flatten:
            relation = flatten.flatten(relation)

        output_file = self.artifact_path(asof_seconds, f".{self._output_format.value}")

        match self._output_format:
            case OutputFormat.CSV:
                relation.to_csv(file_name=str(output_file))

            case OutputFormat.PARQUET:
                relation.to_parquet(file_name=str(output_file))

        logging.info("Wrote %s", output_file)

        return [output_file]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = WriteTopicsToFile
