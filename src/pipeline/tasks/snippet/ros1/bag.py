"""Create a new ROS1 bag snippet using the `rosbag filter` CLI tool."""

import heapq
import logging
import pathlib
import shlex
import subprocess

from src.di import module
from src.pipeline import base, messages


class SnipRosbag(base.ArtifactMixin, messages.TopicMessageMixin, base.Task):
    """Create a new ROS1 bag snippet using the `rosbag filter` CLI tool."""

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
        conditions = []

        topics, data_source = self._topics, None
        if topics is None:
            data_source = self.factory.build()
            topics = self.registry.available_topics(data_source)

        if topics:
            condition = " or ".join(f"topic == '{topic}'" for topic in topics)
            conditions.append(f"({condition})")

        match lookback:
            case base.Lookback(last=int(last), unit=base.Unit.FRAME):
                timestamps = []
                data_source = data_source or self.factory.build()
                connections = data_source._get_connections(topics=topics)
                for indexes in data_source._get_indexes(connections=connections):
                    for index in indexes:
                        timestamp_seconds = index.time.to_sec()
                        if timestamp_seconds <= asof_seconds:
                            heapq.heappush(timestamps, timestamp_seconds)
                start_seconds = timestamps[-last] if len(timestamps) >= last else timestamps[0]
                conditions.append(f"{start_seconds} <= t.to_sec() <= {asof_seconds}")
            case base.Lookback(last=_, unit=_):
                start_seconds = asof_seconds - lookback.to_seconds()
                end_seconds = asof_seconds + self._post_seconds
                conditions.append(f"{start_seconds} <= t.to_sec() <= {end_seconds}")
            case _:
                end_seconds = asof_seconds + self._post_seconds
                conditions.append(f"t.to_sec() <= {end_seconds}")

        output_file = self.artifact_path(asof_seconds, ".bag")

        command = [
            "rosbag",
            "filter",
            str(self.factory.path),
            str(output_file),
            " and ".join(conditions),
        ]

        result = subprocess.run(  # noqa: S603
            command,
            check=True,  # raise CalledProcessError if nonzero exit
            text=True,
            capture_output=True,
        )

        logging.debug(shlex.join(result.args))
        logging.debug(result.stdout.strip())
        logging.info("Wrote %s", output_file)

        return [output_file]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SnipRosbag
