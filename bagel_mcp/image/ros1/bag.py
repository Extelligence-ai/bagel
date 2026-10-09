"""An image dataset for ROS1 bags."""

from collections.abc import Iterator

from PIL import Image

from bagel_mcp import bags
from bagel_mcp.di import module
from bagel_mcp.image import base
from bagel_mcp.image.ros1 import decode

FEATURE = "Reading ROS 1 .bag images"


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
        data_source: bags.Reader,
        topics: list[str],
        start_seconds_inclusive: float | None,
        end_seconds_inclusive: float | None,
    ) -> Iterator[tuple[str, float, Image.Image]]:
        """Return an iterator of topic name, timestamp in seconds, and images."""
        for topic, timestamp_ns, message in data_source.messages(
            topics, start_seconds_inclusive, end_seconds_inclusive
        ):
            image = decode.to_pil(
                message.encoding,
                message.height,
                message.width,
                message.step,
                message.data,
                bigendian=bool(message.is_bigendian),
            )
            yield topic, bags.base.ros1_seconds(timestamp_ns), image


def register() -> None:
    """Register module for dependency injection (needs a bag backend: rosbags or native ROS 1)."""
    bags.require(FEATURE, ros_version=1)
    module.global_registry[__name__] = ImageDataset
