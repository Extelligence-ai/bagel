"""An image dataset reading ROS1 image messages from a Bagel sink."""

import base64
from typing import Any

import cv2
from cv_bridge import CvBridge
from PIL import Image as PILImage
from sensor_msgs.msg import Image

from src.di import module
from src.image.bagel import sink

bridge = CvBridge()


class ImageDataset(sink.ImageDataset):
    """An image dataset reading ROS1 image messages from a Bagel sink."""

    @property
    def image_type_name(self) -> str:
        """ROS1 image type name."""
        return "sensor_msgs/Image"

    def _to_image(self, msg: dict[str, Any]) -> PILImage.Image:
        """Cast a message dictionary into a PIL.Image object."""
        image = Image()
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

        cv_image = bridge.imgmsg_to_cv2(image, desired_encoding="passthrough")

        if image.encoding.lower() in ("bgr8", "bgr16"):
            cv_image = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)

        return PILImage.fromarray(cv_image)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ImageDataset
