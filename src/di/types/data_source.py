"""A list of supported data source types and utilities to identify them."""

import pathlib
from enum import Enum
from urllib.parse import urlparse

import yaml


class DataSource(Enum):
    """Supported data source types."""

    ROS1_BAG = "ros1.bag"
    ROS2_DB3 = "ros2.db3"
    ROS2_MCAP = "ros2.mcap"
    PX4_ULOG = "px4.ulg"
    ARDUPILOT_BIN = "ardupilot.bin"
    BETAFLIGHT_BBL = "betaflight.bbl"
    BETAFLIGHT_BFL = "betaflight.bfl"
    BAGEL_SINK = "bagel.sink"


def resolve(path: str) -> DataSource:
    """Resolve the data source type from the given path or URL."""
    result = urlparse(path)
    if all([result.scheme, result.netloc]):
        # path is a URL
        raise NotImplementedError("Stream-based data sources are not supported yet.")
    else:
        # path is a local file or directory
        return resolve_file_based_data_source(path)


def resolve_file_based_data_source(path: str | pathlib.Path) -> DataSource:  # noqa: PLR0911
    """Resolve the data source type from the given file or directory path."""
    path = pathlib.Path(path)
    if is_bagel_sink_directory(path):
        return DataSource.BAGEL_SINK
    elif is_ros1_bag_file(path):
        return DataSource.ROS1_BAG
    elif is_ros2_db3_file(path) or is_ros2_db3_zstd_file(path) or is_ros2_db3_directory(path):
        return DataSource.ROS2_DB3
    elif is_ros2_mcap_file(path) or is_ros2_mcap_zstd_file(path) or is_ros2_mcap_directory(path):
        return DataSource.ROS2_MCAP
    elif is_px4_ulog_file(path):
        return DataSource.PX4_ULOG
    elif is_ardupilot_bin_file(path):
        return DataSource.ARDUPILOT_BIN
    elif is_betaflight_bbl_file(path):
        return DataSource.BETAFLIGHT_BBL
    elif is_betaflight_bfl_file(path):
        return DataSource.BETAFLIGHT_BFL
    else:
        raise ValueError(f"Cannot resolve data source type from path: {path}")


def has_magic_bytes(path: pathlib.Path, magic: bytes) -> bool:
    """Check if the given path has the specified magic bytes at the beginning."""
    if not path.is_file():
        return False

    try:
        with open(path, "rb") as f:
            head = f.read(len(magic))
        return head == magic
    except OSError:
        return False


def is_zstd_file(path: pathlib.Path) -> bool:
    """Check if the given path is a Zstandard compressed file."""
    return has_magic_bytes(path, b"\x28\xb5\x2f\xfd")


def is_ros1_bag_file(path: pathlib.Path) -> bool:
    """Check if the given path is a ROS 1 bag file."""
    return has_magic_bytes(path, b"#ROSBAG V2")


def is_ros2_db3_file(path: pathlib.Path) -> bool:
    """Check if the given path is a ROS 2 DB3 file."""
    return has_magic_bytes(path, b"SQLite format 3\000")


def is_ros2_db3_zstd_file(path: pathlib.Path) -> bool:
    """Check if the given path is a ROS 2 DB3 Zstandard compressed file."""
    return is_zstd_file(path) and str(path).endswith(".db3.zstd")


def is_ros2_db3_directory(path: pathlib.Path) -> bool:
    """Check if the given path is a directory containing ROS 2 DB3 files."""
    if not path.is_dir():
        return False

    metadata_file = path / "metadata.yaml"
    if not metadata_file.exists():
        return False

    metadata = yaml.safe_load(metadata_file.read_text())
    if metadata["rosbag2_bagfile_information"]["storage_identifier"] not in {"sqlite3", ""}:
        return False

    for relative_path in metadata["rosbag2_bagfile_information"]["relative_file_paths"]:
        file = path / relative_path
        if not file.exists() or not (is_ros2_db3_file(file) or is_ros2_db3_zstd_file(file)):
            return False

    return True


def is_ros2_mcap_file(path: pathlib.Path) -> bool:
    """Check if the given path is a ROS 2 MCAP file."""
    return has_magic_bytes(path, b"\x89MCAP0\r\n")


def is_ros2_mcap_zstd_file(path: pathlib.Path) -> bool:
    """Check if the given path is a ROS 2 MCAP Zstandard compressed file."""
    return is_zstd_file(path) and str(path).endswith(".mcap.zstd")


def is_ros2_mcap_directory(path: pathlib.Path) -> bool:
    """Check if the given path is a directory containing ROS 2 MCAP files."""
    if not path.is_dir():
        return False

    metadata_file = path / "metadata.yaml"
    if not metadata_file.exists():
        return False

    metadata = yaml.safe_load(metadata_file.read_text())
    if metadata["rosbag2_bagfile_information"]["storage_identifier"] not in {"mcap", ""}:
        return False

    for relative_path in metadata["rosbag2_bagfile_information"]["relative_file_paths"]:
        file = path / relative_path
        if not file.exists() or not (is_ros2_mcap_file(file) or is_ros2_mcap_zstd_file(file)):
            return False

    return True


def is_px4_ulog_file(path: pathlib.Path) -> bool:
    """Check if the given path is a PX4 ULog file."""
    return has_magic_bytes(path, b"ULog")


def is_ardupilot_bin_file(path: pathlib.Path) -> bool:
    """Check if the given path is an ArduPilot binary Dataflash file."""
    return has_magic_bytes(path, b"\xa3\x95\x80")


def is_betaflight_bbl_file(path: pathlib.Path) -> bool:
    """Check if the given path is a Betaflight .bbl file."""
    return has_magic_bytes(path, b"H Product:") and path.suffix.lower() == ".bbl"


def is_betaflight_bfl_file(path: pathlib.Path) -> bool:
    """Check if the given path is a Betaflight .BFL file."""
    return has_magic_bytes(path, b"H Product:") and path.suffix.lower() == ".bfl"


def is_bagel_sink_directory(path: pathlib.Path) -> bool:
    """Check if the given path is a directory containing a Bagel topic sink."""
    if not path.is_dir():
        return False

    metadata_file = path / "metadata.yaml"
    if not metadata_file.exists():
        return False
    metadata = yaml.safe_load(metadata_file.read_text())

    magic = metadata.get("magic")
    if magic != "BAGEL_SINK":
        return False

    return True
