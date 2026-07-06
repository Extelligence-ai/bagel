"""Create a new ROS2 DB3 bag snippet."""

import logging
import pathlib
from collections import deque

import rosbag2_py
from rclpy.serialization import serialize_message

from src import artifacts
from src.di import module
from src.pipeline import base, messages

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class SnipRosbag(messages.TopicMessageMixin, base.Task):
    """Create a new ROS2 DB3 bag snippet."""

    def __init__(
        self,
        topics: list[str] | None = None,
        output_serialization_format: str = "cdr",
        post_seconds: float = 0.0,
    ) -> None:
        """Initialize the task.

        Args:
            topics (list[str] | None, optional): A list of topics to filter. If None, all available
                topics will be written to the new bag file.
            output_serialization_format (str, optional): The serialization format for the output.
                Defaults to "cdr".
            post_seconds (float, optional): How many seconds *after* `asof_seconds` to also include
                in the snippet. Combined with a time-based `lookback` (the pre-window) this produces
                a symmetric window around an event, e.g. lookback=10s + post_seconds=10 keeps
                [asof - 10s, asof + 10s]. Defaults to 0.0 (pre-window only). Ignored for
                frame-based lookbacks. Must be non-negative.

        Raises:
            ValueError: If 'topics' is specified but is an empty list.
            ValueError: If 'post_seconds' is negative.

        """
        if topics is not None and len(topics) == 0:
            raise ValueError("If 'topics' is specified, it must contain at least one topic name.")
        if post_seconds < 0:
            raise ValueError(f"'post_seconds' must be non-negative, got {post_seconds}.")
        self._topics = topics
        self._output_serialization_format = output_serialization_format
        self._post_seconds = post_seconds

        self._output_storage_id = "sqlite3"

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""
        data_source = self.factory.build()
        topics = self._topics or self.registry.available_topics(data_source)

        end_seconds = asof_seconds + self._post_seconds

        match lookback:
            case base.Lookback(last=int(last), unit=base.Unit.FRAME):
                messages = deque(maxlen=last)
                for tup in self.dataset._messages(data_source, topics, None, asof_seconds):
                    messages.append(tup)
            case base.Lookback(last=_, unit=_):
                start_seconds = asof_seconds - lookback.to_seconds()
                messages = self.dataset._messages(data_source, topics, start_seconds, end_seconds)
            case _:
                messages = self.dataset._messages(data_source, topics, None, end_seconds)

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
            for topic, timestamp_seconds, message in messages:
                writer.write(topic, serialize_message(message), int(timestamp_seconds * SECOND))
        finally:
            writer.close()

        logging.info("Wrote %s", bag_directory)

        return [bag_directory]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SnipRosbag
