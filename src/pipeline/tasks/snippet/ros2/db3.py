"""Create a new ROS2 DB3 bag snippet."""

import logging
import pathlib
from collections import deque

import rosbag2_py
from rclpy.serialization import serialize_message

from src.artifacts import short_digest
from src.di import module
from src.pipeline import base, messages

NANOSECOND = 1
MICROSECOND = 1_000 * NANOSECOND
MILLISECOND = 1_000 * MICROSECOND
SECOND = 1_000 * MILLISECOND


class SnipRosbagTask(messages.TopicMessageMixin, base.Task):
    """Create a new ROS2 DB3 bag snippet."""

    def __init__(
        self,
        topics: list[str] | None,
        output_directory: str,
        output_serialization_format: str = "cdr",
    ) -> None:
        """Initialize the task.

        Args:
            topics (list[str] | None): A list of topics to filter. If None, all available
                topics will be written to the new bag file.
            output_directory (str): The directory to write the new bag directories to.
            output_serialization_format (str, optional): The serialization format for the output.
                Defaults to "cdr".

        Raises:
            ValueError: If 'topics' is specified but is an empty list.

        """
        if topics is not None and len(topics) == 0:
            raise ValueError("If 'topics' is specified, it must contain at least one topic name.")
        self._topics = topics
        self._output_directory = pathlib.Path(output_directory)
        self._output_serialization_format = output_serialization_format

        self._output_storage_id = "sqlite3"

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        """Execute the task at the given time."""
        data_source = self.factory.build()
        topics = self._topics or self.registry.available_topics(data_source)

        match lookback:
            case base.Lookback(last=int(last), unit=base.Unit.FRAME):
                messages = deque(maxlen=last)
                for tup in self.dataset._messages(data_source, topics, None, asof_seconds):
                    messages.append(tup)
            case base.Lookback(last=_, unit=_):
                start_seconds = asof_seconds - lookback.to_seconds()
                messages = self.dataset._messages(data_source, topics, start_seconds, asof_seconds)
            case _:
                messages = self.dataset._messages(data_source, topics, None, asof_seconds)

        digest = short_digest(topics)
        bag_directory = self._output_directory / f"timestamp_seconds={asof_seconds}" / digest
        bag_directory.parent.mkdir(parents=True, exist_ok=True)

        storage_options = rosbag2_py.StorageOptions(
            uri=str(bag_directory), storage_id=self._output_storage_id
        )
        converter_options = rosbag2_py.ConverterOptions("", "")

        n_message_written = 0
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
                n_message_written += 1
        finally:
            writer.close()

        logging.info("Wrote %d messages to %s", n_message_written, bag_directory)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SnipRosbagTask
