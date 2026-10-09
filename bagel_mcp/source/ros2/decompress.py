"""Decompress a single zstd-compressed ROS 2 storage file so a reader can open it.

A ``.db3.zstd`` / ``.mcap.zstd`` *file* has no metadata.yaml to tell a reader how to
undo the compression, so it is expanded into the cache (keyed by the source's path,
size and mtime; the source is never touched) the way the format-agnostic MCAP source
does. Compressed bag *directories* (``compression_mode: file`` or ``message``) are
left alone: both bag backends read those as they are.
"""

import pathlib

from bagel_mcp.source import mcap


def ros2bag(path: pathlib.Path) -> pathlib.Path:
    """Return a path a bag reader can open, decompressing a lone ``.zstd`` file if needed."""
    path = pathlib.Path(path)
    if path.is_file() and path.suffix == ".zstd":
        return mcap.decompress(path)
    return path
