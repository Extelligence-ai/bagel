"""Utility functions with respect to robologs."""

import functools
import hashlib
import pathlib
import uuid
from enum import Enum

import yaml

BYTE = 1
KB = 1024 * BYTE
MB = 1024 * KB


class UnsupportedRobologTypeError(Exception):
    """Raised when a robolog type is not supported."""


class RobologType(Enum):
    """Represent supported robolog types."""

    ROS1_BAG_FILE = "ros1_bag_file"
    ROS2_DB3_FILE = "ros2_db3_file"
    ROS2_MCAP_FILE = "ros2_mcap_file"
    ROS2_DB3_DIR = "ros2_db3_dir"  # contain metadata.yaml and .db3 files
    ROS2_MCAP_DIR = "ros2_mcap_dir"  # contain metadata.yaml and .mcap files
    PX4_ULG_FILE = "px4_ulg_file"
    ARDUPILOT_BIN_FILE = "ardupilot_bin_file"  # ArduPilot Dataflash binary log file


@functools.lru_cache(maxsize=128)
def detect_robolog_type(robolog_path: str | pathlib.Path) -> RobologType:  # noqa: C901, PLR0911
    """Detect the robolog type by analyzing its path and content."""
    path = pathlib.Path(robolog_path).absolute()

    if not path.exists():
        raise FileNotFoundError(robolog_path)

    if path.is_file():
        match path.suffix:
            case ".bag":
                return RobologType.ROS1_BAG_FILE
            case ".db3":
                return RobologType.ROS2_DB3_FILE
            case ".mcap":
                return RobologType.ROS2_MCAP_FILE
            case ".ulg":
                return RobologType.PX4_ULG_FILE
            case ".bin":
                return RobologType.ARDUPILOT_BIN_FILE

    elif path.is_dir() and (path / "metadata.yaml").exists():  # ROS2 bag directory
        metadata = yaml.safe_load((path / "metadata.yaml").read_text())
        relative_file_paths = metadata["rosbag2_bagfile_information"]["relative_file_paths"]
        files_missing = [file for file in relative_file_paths if not (path / file).exists()]
        if files_missing:
            raise FileNotFoundError(files_missing)
        if all(file.endswith(".db3") for file in relative_file_paths):
            return RobologType.ROS2_DB3_DIR
        elif all(file.endswith(".mcap") for file in relative_file_paths):
            return RobologType.ROS2_MCAP_DIR

    raise UnsupportedRobologTypeError(robolog_path)


def _md5_first_64mb(file: str | pathlib.Path) -> str:
    """Calculate the MD5 hash of a file based on the first 64 MB of its content."""
    hash_func = hashlib.new("md5")  # noqa: S324
    with open(file, "rb") as f:
        hash_func.update(f.read(64 * MB))  # don't touch the number
    return hash_func.hexdigest()


@functools.lru_cache(maxsize=128)
def generate_id(robolog_path: str | pathlib.Path) -> str:
    """Generate a deterministic UUID for a robolog based on its absolute path and content."""
    absolute_path = pathlib.Path(robolog_path).absolute()

    content_hashes = []
    if absolute_path.is_file():
        content_hashes.append(_md5_first_64mb(absolute_path))
    else:
        for path in sorted(absolute_path.glob("**/*")):
            if path.is_file():
                content_hashes.append(_md5_first_64mb(path.absolute()))

    return str(uuid.uuid5(uuid.NAMESPACE_OID, "_".join(content_hashes)))


def snippet_name(robolog_path: str | pathlib.Path, start_seconds: float, end_seconds: float) -> str:
    """Generate a name for a robolog snippet."""
    return f"snippet_{str(generate_id(robolog_path))[:8]}_{start_seconds!s}_{end_seconds!s}"
