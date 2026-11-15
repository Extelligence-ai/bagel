"""Reduce a ROS2 DB3 bag to only the data around detected events."""

import dataclasses
import logging
import pathlib

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.pipeline import base, messages
from bagel_mcp.pipeline.tasks.reduce.base import ReduceMixin, check_options

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
        event_topic: str | None = None,
        predicate: str | None = None,
        pre_seconds: float = 0.0,
        post_seconds: float = 0.0,
        debounce_seconds: float = 0.0,
        topics: list[str] | None = None,
        exclude_topics: list[str] | None = None,
        output_serialization_format: str = "cdr",
    ) -> None:
        """Initialize the task.

        Args:
            event_topic (str | None, optional): The topic whose messages are tested
                against ``predicate``. Omit with ``predicate`` to keep the whole time range
                (e.g. only to strip topics). Defaults to None.
            predicate (str | None, optional): A SQL boolean expression over
                ``event_topic`` columns (the same table contract as the ``gates.sql``
                gate), e.g. ``"linear_acceleration_x < -10"``. An event is the rising
                edge where the predicate transitions from False to True. Defaults to None.
            pre_seconds (float, optional): Seconds to keep *before* each event.
                Defaults to 0.0.
            post_seconds (float, optional): Seconds to keep *after* each event.
                Defaults to 0.0.
            debounce_seconds (float, optional): Minimum seconds between consecutive
                events; closer events are coalesced. Defaults to 0.0 (no debounce).
            topics (list[str] | None, optional): Topics to write to the reduced bag. If
                None, all available topics are written. Defaults to None.
            exclude_topics (list[str] | None, optional): Topics to drop instead; every
                other topic is written. Not together with ``topics``. Defaults to None.
            output_serialization_format (str, optional): Serialization format for the
                output. Defaults to "cdr".

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
        self._output_serialization_format = output_serialization_format

        self._output_storage_id = "sqlite3"

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""
        data_source = self.factory.build()
        topics = self._kept_topics(self.registry.available_topics(data_source))

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
