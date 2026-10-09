"""An image dataset for ROS1 bags."""

from __future__ import annotations

import functools
from collections.abc import Iterator

import cv2
from PIL import Image

from bagel_mcp import ros_native
from bagel_mcp.di import module
from bagel_mcp.image import base

FEATURE = "Reading ROS 1 .bag images"
rosbag = ros_native.optional("rosbag", feature=FEATURE)
genpy = ros_native.optional("genpy", feature=FEATURE)
cv_bridge = ros_native.optional("cv_bridge", feature=FEATURE)


@functools.cache
def _bridge() -> object:
    """Build the cv_bridge converter on first use, not at import."""
    return cv_bridge.CvBridge()


class ImageDataset(base.ImageDataset):
    """An image dataset for ROS1 bags."""

    def __init__(self) -> None:
        """Initialize the image dataset for ROS1 bags."""
        super().__init__(use_cache=False)  # ROS1 bags can seek time ranges. No cache needed.

    @property
    def image_type_name(self) -> str:
        """ROS1 image type name."""
        return "sensor_msgs/Image"

    def _images(
        self,
        data_source: rosbag.Bag,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, Image.Image]]:
        """Return an iterator of topic name, timestamp in seconds, and images."""
        messages = data_source.read_messages(
            topics,
            genpy.Time.from_sec(start_seconds_inclusive) if start_seconds_inclusive else None,
            genpy.Time.from_sec(end_seconds_inclusive) if end_seconds_inclusive else None,
        )

        for topic, message, timestamp in messages:
            cv_image = _bridge().imgmsg_to_cv2(message, desired_encoding="passthrough")
            if message.encoding.lower() in ("bgr8", "bgr16"):
                cv_image = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)
            yield topic, timestamp.to_sec(), Image.fromarray(cv_image)


def register() -> None:
    """Register module for dependency injection (only where native ROS 1 is present)."""
    ros_native.require("cv_bridge", feature=FEATURE)
    module.global_registry[__name__] = ImageDataset
