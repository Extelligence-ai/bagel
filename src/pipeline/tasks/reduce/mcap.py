"""Reduce an MCAP file to only the data around detected events.

Format-agnostic: works on any MCAP source (ros1, ros2, protobuf, ... profiles) and
requires no ROS installation. Writes MCAP by copying raw message records within the
kept windows -- it never decodes and re-encodes messages, so it does not need rosidl
typesupport (the reason writing MCAP via ``serialize_message`` fails). Event detection
still uses the decoded path (``to_duckdb``); only the write is a raw byte passthrough.
"""

import logging
import pathlib

from src.di import module
from src.pipeline import base, mcap_raw, messages
from src.pipeline.tasks.reduce.base import ReduceMixin

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class ReduceMcap(base.ArtifactMixin, ReduceMixin, messages.TopicMessageMixin, base.Task):
    """Reduce a ROS2 MCAP bag to only the windows around events matching a predicate.

    Like the db3 reduce task, but writes a single reduced ``.mcap`` file by copying raw
    message records (schema and channel definitions are copied verbatim), so no message
    serialization is required.
    """

    def __init__(  # noqa: PLR0913
        self,
        event_topic: str,
        predicate: str,
        pre_seconds: float,
        post_seconds: float = 0.0,
        debounce_seconds: float = 0.0,
        topics: list[str] | None = None,
    ) -> None:
        """Initialize the task.

        Args:
            event_topic (str): The topic whose messages are tested against ``predicate``.
            predicate (str): A SQL boolean expression over ``event_topic`` columns, e.g.
                ``"imu['linear_acceleration']['x'] < -10"``.
            pre_seconds (float): Seconds to keep before each event.
            post_seconds (float, optional): Seconds to keep after each event. Defaults to 0.0.
            debounce_seconds (float, optional): Minimum seconds between consecutive events.
                Defaults to 0.0.
            topics (list[str] | None, optional): Topics to keep in the reduced bag. If None,
                all topics present in the source are kept. Defaults to None.

        Raises:
            ValueError: If 'topics' is specified but empty.
            ValueError: If any of pre_seconds, post_seconds, or debounce_seconds is negative.

        """
        if topics is not None and len(topics) == 0:
            raise ValueError("If 'topics' is specified, it must contain at least one topic name.")
        if pre_seconds < 0 or post_seconds < 0:
            raise ValueError("'pre_seconds' and 'post_seconds' must be non-negative.")
        if debounce_seconds < 0:
            raise ValueError(f"'debounce_seconds' must be non-negative, got {debounce_seconds}.")

        self._event_topic = event_topic
        self._predicate = predicate
        self._pre_seconds = pre_seconds
        self._post_seconds = post_seconds
        self._debounce_seconds = debounce_seconds
        self._topics = topics

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""
        _events, intervals = self._kept_intervals(asof_seconds)
        logging.info(
            "Reduce MCAP: %d event(s) -> %d kept window(s) on topic '%s'",
            len(_events),
            len(intervals),
            self._event_topic,
        )
        intervals_ns = [(int(start * SECOND), int(end * SECOND)) for start, end in intervals]

        data_source = self.factory.build()
        output_file = self.artifact_path(asof_seconds, ".mcap")

        records = (
            record
            for record in mcap_raw.iter_raw_messages(data_source.mcap_files, self._topics)
            if mcap_raw.in_intervals(record[2].log_time, intervals_ns)
        )
        mcap_raw.write_raw_messages(output_file, records)

        logging.info("Wrote %s", output_file)

        return [output_file]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ReduceMcap
