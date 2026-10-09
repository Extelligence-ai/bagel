"""Create a new ROS1 bag snippet."""

import dataclasses
import logging
import pathlib
from collections import deque

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.pipeline import base, messages

FEATURE = "Writing ROS 1 .bag snippets"


class SnipRosbag(base.ArtifactMixin, messages.TopicMessageMixin, base.Task):
    """Create a new ROS1 bag snippet: the recorded bytes of a window, in a new .bag."""

    needs_recorded_log = True

    def __init__(
        self,
        topics: list[str] | None = None,
        post_seconds: float = 0.0,
    ) -> None:
        """Initialize the task.

        Args:
            topics (list[str] | None, optional): A list of topics to filter. If None, all available
                topics will be written to the new bag file.
            post_seconds (float, optional): How many seconds *after* `asof_seconds` to also include
                in the snippet. Combined with a time-based `lookback` (the pre-window) this produces
                a symmetric window around an event, e.g. lookback=10s + post_seconds=10 keeps
                [asof - 10s, asof + 10s]. Defaults to 0.0 (pre-window only). Ignored for
                frame-based lookbacks. Must be non-negative.

        Raises:
            ValueError: If the topics list is empty when specified.
            ValueError: If 'post_seconds' is negative.

        """
        if topics is not None and len(topics) == 0:
            raise ValueError("If 'topics' is specified, it must contain at least one topic name.")
        if post_seconds < 0:
            raise ValueError(f"'post_seconds' must be non-negative, got {post_seconds}.")
        self._topics = topics
        self._post_seconds = post_seconds

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> list[pathlib.Path]:
        """Execute the task at the given time."""
        data_source = self.factory.build()
        topics = self._topics or self.registry.available_topics(data_source)

        end_seconds = asof_seconds + self._post_seconds

        match lookback:
            case base.Lookback(last=int(last), unit=base.Unit.FRAME):
                records = deque(maxlen=last)
                for record in data_source.raw_messages(topics, None, asof_seconds):
                    records.append(record)
            case base.Lookback(last=_, unit=_):
                start_seconds = asof_seconds - lookback.to_seconds()
                records = data_source.raw_messages(topics, start_seconds, end_seconds)
            case _:
                records = data_source.raw_messages(topics, None, end_seconds)

        output_file = self.artifact_path(asof_seconds, ".bag")

        # Serialized bytes are copied as recorded: no deserialize/serialize round trip.
        with bags.open_writer(output_file, ros_version=1) as writer:
            for topic in topics:
                writer.add_topic(
                    dataclasses.replace(
                        data_source.info.topic(topic), definition=data_source.definition(topic)
                    )
                )
            for topic, timestamp_ns, data in records:
                writer.write(topic, timestamp_ns, data)

        logging.info("Wrote %s", output_file)

        return [output_file]


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 1)."""
    bags.require(FEATURE, ros_version=1)
    module.global_registry[__name__] = SnipRosbag
