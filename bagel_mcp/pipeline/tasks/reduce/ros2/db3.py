"""Reduce a ROS2 DB3 bag to only the data around detected events."""

import dataclasses
import logging
import pathlib

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.pipeline import base, messages
from bagel_mcp.pipeline.tasks.reduce.base import ReduceMixin

FEATURE = "Writing reduced ROS 2 .db3 bags"


class ReduceRosbag(base.ArtifactMixin, ReduceMixin, messages.TopicMessageMixin, base.Task):
    """Reduce a ROS2 DB3 bag to only the windows around events matching a predicate.

    Unlike the ``snippet`` task -- which fires once per event and writes one clip per
    event -- this task writes a **single** new bag. It detects every event on
    ``event_topic`` where ``predicate`` rises from False to True, builds
    ``[event - pre_seconds, event + post_seconds]`` windows, merges overlapping
    windows, and copies only the messages that fall inside the kept windows.
    Everything outside the kept windows is discarded.
    """

    needs_recorded_log = True

    def __init__(  # noqa: PLR0913
        self,
        event_topic: str,
        predicate: str,
        pre_seconds: float,
        post_seconds: float = 0.0,
        debounce_seconds: float = 0.0,
        topics: list[str] | None = None,
        output_serialization_format: str = "cdr",
    ) -> None:
        """Initialize the task.

        Args:
            event_topic (str): The topic whose messages are tested against ``predicate``.
            predicate (str): A SQL boolean expression over ``event_topic`` columns
                (the same table contract as the ``gates.sql`` gate), e.g.
                ``"linear_acceleration_x < -10"``. An event is the rising edge where the
                predicate transitions from False to True.
            pre_seconds (float): Seconds to keep *before* each event.
            post_seconds (float, optional): Seconds to keep *after* each event.
                Defaults to 0.0.
            debounce_seconds (float, optional): Minimum seconds between consecutive
                events; closer events are coalesced. Defaults to 0.0 (no debounce).
            topics (list[str] | None, optional): Topics to write to the reduced bag. If
                None, all available topics are written. Defaults to None.
            output_serialization_format (str, optional): Serialization format for the
                output. Defaults to "cdr".

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
        self._output_serialization_format = output_serialization_format

        self._output_storage_id = "sqlite3"

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""
        data_source = self.factory.build()
        topics = self._topics or self.registry.available_topics(data_source)

        events, intervals = self._kept_intervals(asof_seconds)
        logging.info(
            "Reduce: %d event(s) -> %d kept window(s) on topic '%s'",
            len(events),
            len(intervals),
            self._event_topic,
        )

        bag_directory = self.artifact_path(asof_seconds)

        # Serialized bytes are copied as recorded: no deserialize/serialize round trip.
        with bags.open_writer(
            bag_directory, ros_version=2, storage=self._output_storage_id
        ) as writer:
            for topic in topics:
                writer.add_topic(
                    dataclasses.replace(
                        data_source.info.topic(topic),
                        definition=data_source.definition(topic),
                        serialization_format=self._output_serialization_format,
                    )
                )
            for start_seconds, end_seconds in intervals:
                for topic, timestamp_ns, data in data_source.raw_messages(
                    topics, start_seconds, end_seconds
                ):
                    writer.write(topic, timestamp_ns, data)

        logging.info("Wrote %s", bag_directory)

        return [bag_directory]


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 2)."""
    bags.require(FEATURE, ros_version=2)
    module.global_registry[__name__] = ReduceRosbag
