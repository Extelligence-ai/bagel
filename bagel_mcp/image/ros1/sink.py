"""An image dataset reading ROS1 image messages from a Bagel sink."""

import base64
from typing import Any

from PIL import Image as PILImage

from bagel_mcp.di import module
from bagel_mcp.image.bagel import sink
from bagel_mcp.image.ros1 import decode


class ImageDataset(sink.ImageDataset):
    """An image dataset reading ROS1 image messages from a Bagel sink."""

    @property
    def image_type_name(self) -> str:
        """ROS1 image type name."""
        return "sensor_msgs/Image"

    def _to_image(self, msg: dict[str, Any]) -> PILImage.Image:
        """Cast a message dictionary (rosbridge JSON) into a PIL.Image object."""
        data_field = msg["data"]
        data = base64.b64decode(data_field) if isinstance(data_field, str) else bytes(data_field)
        return decode.to_pil(
            msg["encoding"],
            msg["height"],
            msg["width"],
            msg["step"],
            data,
            bigendian=bool(msg.get("is_bigendian", 0)),
        )


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = ImageDataset
