"""Decode ``sensor_msgs/Image`` pixels into PIL images without ``cv_bridge``.

A raw ROS image is a row-major byte buffer with ``step`` bytes per row; this maps the
common encodings onto numpy dtypes and channel counts and reorders BGR to RGB, which
is all ``cv_bridge.imgmsg_to_cv2(..., "passthrough")`` plus ``cv2.cvtColor`` did for
Bagel. Compressed images are a different message type and are not handled here.
"""

from __future__ import annotations

import numpy as np
from PIL import Image

# encoding -> (numpy dtype, channels, PIL mode or None for the default, swap BGR->RGB)
_ENCODINGS: dict[str, tuple[str, int, str | None, bool]] = {
    "mono8": ("u1", 1, "L", False),
    "8uc1": ("u1", 1, "L", False),
    "mono16": ("u2", 1, "I;16", False),
    "16uc1": ("u2", 1, "I;16", False),
    "rgb8": ("u1", 3, "RGB", False),
    "bgr8": ("u1", 3, "RGB", True),
    "8uc3": ("u1", 3, "RGB", False),
    "rgba8": ("u1", 4, "RGBA", False),
    "bgra8": ("u1", 4, "RGBA", True),
    "8uc4": ("u1", 4, "RGBA", False),
}


class UnsupportedImageEncodingError(ValueError):
    """The image encoding has no mapping to a PIL mode."""


def to_pil(  # noqa: PLR0913 -- one argument per sensor_msgs/Image field
    encoding: str,
    height: int,
    width: int,
    step: int,
    data: bytes | bytearray | memoryview | np.ndarray,
    *,
    bigendian: bool = False,
) -> Image.Image:
    """Decode one ``sensor_msgs/Image`` payload.

    Args:
        encoding: The message's ``encoding`` field (``rgb8``, ``bgr8``, ``mono16``, ...).
        height: Rows.
        width: Columns.
        step: Bytes per row, including any padding.
        data: The pixel buffer.
        bigendian: The message's ``is_bigendian`` flag; matters for 16-bit encodings.

    Raises:
        UnsupportedImageEncodingError: For encodings outside the mapped set.

    """
    key = encoding.lower()
    if key not in _ENCODINGS:
        raise UnsupportedImageEncodingError(encoding)
    dtype, channels, mode, swap = _ENCODINGS[key]
    if dtype == "u2":
        dtype = ">u2" if bigendian else "<u2"
    raw = data.tobytes() if isinstance(data, np.ndarray) else bytes(data)
    buffer = np.frombuffer(raw, dtype=dtype)
    itemsize = np.dtype(dtype).itemsize
    rows = buffer.reshape(height, step // itemsize)[:, : width * channels]
    pixels = rows.reshape(height, width, channels) if channels > 1 else rows.reshape(height, width)
    if swap:
        pixels = pixels[:, :, [2, 1, 0, *range(3, channels)]]
    if dtype.endswith("u2"):
        pixels = pixels.astype("<u2")
    return Image.fromarray(np.ascontiguousarray(pixels), mode=mode)
