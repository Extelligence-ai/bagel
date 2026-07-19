"""Utility function that decompresses a ROS2 bag."""

import pathlib

import rosbag2_py
import zstandard as zstd


def ros2bag(path: pathlib.Path) -> pathlib.Path:
    """Decompress a ROS2 bag if needed and return the decompressed path."""
    decompressed_path = path
    if path.is_file():
        if path.suffix == ".zstd":
            decompressed_path = path.with_suffix("")
            with (
                open(path, "rb") as f_in,
                open(decompressed_path, "wb") as f_out,
            ):
                zstd.ZstdDecompressor().copy_stream(f_in, f_out)
    elif path.is_dir():
        metadata = rosbag2_py.Info().read_metadata(str(path), "")
        if metadata.compression_format == "zstd":
            for rel_file, file_info in zip(
                metadata.relative_file_paths, metadata.files, strict=True
            ):
                with (
                    open(path / rel_file, "rb") as f_in,
                    open(path / file_info.path, "wb") as f_out,
                ):
                    zstd.ZstdDecompressor().copy_stream(f_in, f_out)
    return decompressed_path
