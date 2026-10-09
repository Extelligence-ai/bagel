"""An image dataset reading ROS1 image messages from a Bagel sink."""

import base64
import functools
from typing import Any

import cv2
from PIL import Image as PILImage

from bagel_mcp import ros_native
from bagel_mcp.di import module
from bagel_mcp.image.bagel import sink

FEATURE = "Decoding ROS 1 images from a live sink"
cv_bridge = ros_native.optional("cv_bridge", feature=FEATURE)
sensor_msgs = ros_native.optional("sensor_msgs.msg", feature=FEATURE)


@functools.cache
def _bridge() -> object:
    """Build the cv_bridge converter on first use, not at import."""
    return cv_bridge.CvBridge()


class ImageDataset(sink.ImageDataset):
    """An image dataset reading ROS1 image messages from a Bagel sink."""

    @property
    def image_type_name(self) -> str:
        """ROS1 image type name."""
        return "sensor_msgs/Image"

    def _to_image(self, msg: dict[str, Any]) -> PILImage.Image:
        """Cast a message dictionary into a PIL.Image object."""
        image = sensor_msgs.Image()
        image.header.seq = msg["header"]["seq"]
        image.header.stamp.secs = msg["header"]["stamp"]["secs"]
        image.header.stamp.nsecs = msg["header"]["stamp"]["nsecs"]
        image.header.frame_id = msg["header"]["frame_id"]
        image.height = msg["height"]
        image.width = msg["width"]
        image.encoding = msg["encoding"]
        image.is_bigendian = msg["is_bigendian"]
        image.step = msg["step"]

        data_field = msg["data"]
        if isinstance(data_field, str):
            image.data = base64.b64decode(data_field)
        else:
            image.data = bytes(data_field)

        cv_image = _bridge().imgmsg_to_cv2(image, desired_encoding="passthrough")

        if image.encoding.lower() in ("bgr8", "bgr16"):
            cv_image = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)

        return PILImage.fromarray(cv_image)


def register() -> None:
    """Register module for dependency injection (only where native ROS 1 is present)."""
    ros_native.require("cv_bridge", feature=FEATURE)
    module.global_registry[__name__] = ImageDataset
