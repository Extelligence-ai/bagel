"""Create an MCAP snippet: one clip around the current pipeline timestamp.

Format-agnostic and ROS-free: works on any MCAP source (ros1, ros2, protobuf, ...
profiles) by copying raw message records verbatim -- no decode/re-encode, no rosidl
typesupport. Pair it with an `on_event` cadence to write one clip per detected event,
or with a `frequency` cadence for periodic clips.
"""

import logging
import pathlib
from collections import deque

from bagel.di import module
from bagel.pipeline import base, mcap_raw, messages

NANOSECOND = 1
SECOND = 1_000_000_000 * NANOSECOND


class SnipMcap(base.ArtifactMixin, messages.TopicMessageMixin, base.Task):
    """Create a new MCAP snippet around the current pipeline timestamp.

    The clip spans ``[asof - lookback, asof + post_seconds]`` for a time-based
    lookback, the last N messages for a frame-based lookback, or everything up to
    ``asof + post_seconds`` when no lookback is given.
    """

    def __init__(
        self,
        topics: list[str] | None = None,
        post_seconds: float = 0.0,
    ) -> None:
        """Initialize the task.

        Args:
            topics (list[str] | None, optional): A list of topics to include. If None, all
                available topics are written to the snippet.
            post_seconds (float, optional): How many seconds *after* `asof_seconds` to also
                include in the snippet. Combined with a time-based `lookback` (the
                pre-window) this produces a symmetric window around an event, e.g.
                lookback=10s + post_seconds=10 keeps [asof - 10s, asof + 10s].
                Defaults to 0.0 (pre-window only). Ignored for frame-based lookbacks.
                Must be non-negative.

        Raises:
            ValueError: If 'topics' is specified but is an empty list.
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
        raw_messages = mcap_raw.iter_raw_messages(data_source.mcap_files, self._topics)

        end_ns = int((asof_seconds + self._post_seconds) * SECOND)

        match lookback:
            case base.Lookback(last=int(last), unit=base.Unit.FRAME):
                asof_ns = int(asof_seconds * SECOND)
                records = deque(
                    (record for record in raw_messages if record[2].log_time <= asof_ns),
                    maxlen=last,
                )
            case base.Lookback(last=_, unit=_):
                start_ns = int((asof_seconds - lookback.to_seconds()) * SECOND)
                records = (
                    record
                    for record in raw_messages
                    if start_ns <= record[2].log_time <= end_ns
                )
            case _:
                records = (record for record in raw_messages if record[2].log_time <= end_ns)

        output_file = self.artifact_path(asof_seconds, ".mcap")
        count = mcap_raw.write_raw_messages(output_file, records)

        logging.info("Wrote %s (%d messages)", output_file, count)

        return [output_file]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = SnipMcap
