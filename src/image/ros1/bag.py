"""An image dataset for ROS1 bags."""

from collections.abc import Iterator

import cv2
import genpy
import rosbag
from cv_bridge import CvBridge
from PIL import Image

from src.di import module
from src.image import base

bridge = CvBridge()


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
            cv_image = bridge.imgmsg_to_cv2(message, desired_encoding="passthrough")
            if message.encoding.lower() in ("bgr8", "bgr16"):
                cv_image = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)
            yield topic, timestamp.to_sec(), Image.fromarray(cv_image)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ImageDataset
