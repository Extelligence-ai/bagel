"""Reduce an MCAP file to only the data around detected events.

Format-agnostic: works on any MCAP source (ros1, ros2, protobuf, ... profiles) and
requires no ROS installation. Writes MCAP by copying raw message records within the
kept windows -- it never decodes and re-encodes messages, so it does not need rosidl
typesupport (the reason writing MCAP via ``serialize_message`` fails). Event detection
still uses the decoded path (``to_duckdb``); only the write is a raw byte passthrough.
"""

import logging
import pathlib

from bagel_mcp.di import module
from bagel_mcp.pipeline import base, mcap_raw, messages
from bagel_mcp.pipeline.tasks.reduce.base import ReduceMixin, check_options

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

    needs_recorded_log = True

    def __init__(  # noqa: PLR0913
        self,
        event_topic: str | None = None,
        predicate: str | None = None,
        pre_seconds: float = 0.0,
        post_seconds: float = 0.0,
        debounce_seconds: float = 0.0,
        topics: list[str] | None = None,
        exclude_topics: list[str] | None = None,
    ) -> None:
        """Initialize the task.

        Args:
            event_topic (str | None, optional): The topic whose messages are tested
                against ``predicate``. Omit with ``predicate`` to keep the whole time range
                (e.g. only to strip topics). Defaults to None.
            predicate (str | None, optional): A SQL boolean expression over
                ``event_topic`` columns, e.g. ``"imu['linear_acceleration']['x'] < -10"``.
                Defaults to None.
            pre_seconds (float, optional): Seconds to keep before each event. Defaults to 0.0.
            post_seconds (float, optional): Seconds to keep after each event. Defaults to 0.0.
            debounce_seconds (float, optional): Minimum seconds between consecutive events.
                Defaults to 0.0.
            topics (list[str] | None, optional): Topics to keep in the reduced bag. If None,
                all topics present in the source are kept. Defaults to None.
            exclude_topics (list[str] | None, optional): Topics to drop instead (e.g.
                camera or GPS before sharing a log); every other topic is kept. Not
                together with ``topics``. Defaults to None.

        Raises:
            ValueError: If only one of event_topic/predicate is set, both topics and
                exclude_topics are set, either list is empty, or a duration is negative.

        """
        check_options(
            event_topic,
            predicate,
            pre_seconds,
            post_seconds,
            debounce_seconds,
            topics,
            exclude_topics,
        )

        self._event_topic = event_topic
        self._predicate = predicate
        self._pre_seconds = pre_seconds
        self._post_seconds = post_seconds
        self._debounce_seconds = debounce_seconds
        self._topics = topics
        self._exclude_topics = exclude_topics

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""
        data_source = self.factory.build()
        if not hasattr(data_source, "mcap_files"):
            raise ValueError(
                "This reduce task rewrites MCAP files (use reduce.ros2.db3 for .db3 bags). "
                "Flight-controller logs (.ulg, .bin, .bbl) can't be rewritten; export the "
                "topics you need with write_topics_to_file instead."
            )
        topics = self._kept_topics(self.registry.available_topics(data_source))
        _events, intervals = self._kept_intervals(asof_seconds)
        logging.info(
            "Reduce MCAP: %d event(s) -> %d kept window(s) on topic '%s', %d topic(s) kept",
            len(_events),
            len(intervals),
            self._event_topic,
            len(topics),
        )
        whole_log = intervals == [(None, None)]
        intervals_ns = (
            []
            if whole_log
            else [(int(start * SECOND), int(end * SECOND)) for start, end in intervals]
        )

        output_file = self.artifact_path(asof_seconds, ".mcap")

        records = (
            record
            for record in mcap_raw.iter_raw_messages(data_source.mcap_files, topics)
            if whole_log or mcap_raw.in_intervals(record[2].log_time, intervals_ns)
        )
        mcap_raw.write_raw_messages(output_file, records)

        logging.info("Wrote %s", output_file)

        return [output_file]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ReduceMcap
