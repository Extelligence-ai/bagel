"""Reduce a ROS2 DB3 bag to only the data around detected events."""

import logging
import pathlib

import rosbag2_py
from rclpy.serialization import serialize_message

from src import artifacts
from src.di import module
from src.pipeline import base, messages
from src.pipeline.tasks.reduce.base import ReduceMixin

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class ReduceRosbag(ReduceMixin, messages.TopicMessageMixin, base.Task):
    """Reduce a ROS2 DB3 bag to only the windows around events matching a predicate.

    Unlike the ``snippet`` task -- which fires once per event and writes one clip per
    event -- this task writes a **single** new bag. It detects every event on
    ``event_topic`` where ``predicate`` rises from False to True, builds
    ``[event - pre_seconds, event + post_seconds]`` windows, merges overlapping
    windows, and copies only the messages that fall inside the kept windows.
    Everything outside the kept windows is discarded.
    """

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

        bag_directory = artifacts.pipeline_task_artifact_path(
            self.pipeline,
            self.name,
            self.site,
            self.asset,
            self.log_id,
            asof_seconds,
            None,
        )
        bag_directory.parent.mkdir(parents=True, exist_ok=True)

        storage_options = rosbag2_py.StorageOptions(
            uri=str(bag_directory), storage_id=self._output_storage_id
        )
        converter_options = rosbag2_py.ConverterOptions("", "")

        writer = rosbag2_py.SequentialWriter()

        try:
            writer.open(storage_options, converter_options)
            for i, topic in enumerate(topics):
                writer.create_topic(
                    rosbag2_py.TopicMetadata(
                        id=i,
                        name=topic,
                        type=self.registry.native_type_name(topic, data_source),
                        serialization_format=self._output_serialization_format,
                    )
                )
            for start_seconds, end_seconds in intervals:
                for topic, timestamp_seconds, message in self.dataset._messages(
                    data_source, topics, start_seconds, end_seconds
                ):
                    writer.write(topic, serialize_message(message), int(timestamp_seconds * SECOND))
        finally:
            writer.close()

        logging.info("Wrote %s", bag_directory)

        return [bag_directory]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ReduceRosbag
