import tempfile

import pytest

from src.di.types import data_source


def test_should_raise_for_stream() -> None:
    # GIVEN
    path = "http://localhost:9092"

    # WHEN / THEN
    with pytest.raises(
        NotImplementedError, match="Stream-based data sources are not supported yet."
    ):
        data_source.resolve(path)


def test_should_raise_if_file_format_not_supported() -> None:
    # GIVEN
    with tempfile.NamedTemporaryFile(suffix=".pdf") as pdf_file:
        # WHEN / THEN
        with pytest.raises(ValueError, match="Cannot resolve data source type from path:"):
            data_source.resolve(pdf_file.name)


def test_should_resolve_ros1_bag() -> None:
    # GIVEN
    path = "./data/sample/ros1/sample.bag"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ROS1_BAG


def test_should_resolve_ros2_db3_directory() -> None:
    # GIVEN
    path = "./data/sample/ros2/db3"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ROS2_DB3


def test_should_resolve_ros2_db3_file() -> None:
    # GIVEN
    path = "./data/sample/ros2/db3/part_0.db3"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ROS2_DB3


def test_should_resolve_ros2_db3_zstd_file() -> None:
    # GIVEN
    path = "./data/sample/ros2/db3_zstd/part_0.db3.zstd"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ROS2_DB3


def test_should_resolve_ros2_mcap_directory() -> None:
    # GIVEN
    path = "./data/sample/ros2/mcap"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ROS2_MCAP


def test_should_resolve_ros2_mcap_file() -> None:
    # GIVEN
    path = "./data/sample/ros2/mcap/part_0.mcap"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ROS2_MCAP


def test_should_resolve_ros2_mcap_zstd_file() -> None:
    # GIVEN
    path = "./data/sample/ros2/mcap_zstd/part_0.mcap.zstd"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ROS2_MCAP


def test_should_resolve_px4_ulog() -> None:
    # GIVEN
    path = "./data/sample/px4/sample.ulg"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.PX4_ULOG


def test_should_resolve_ardupilot_bin() -> None:
    # GIVEN
    path = "./data/sample/ardupilot/sample.bin"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.ARDUPILOT_BIN


def test_should_resolve_betaflight_bbl() -> None:
    # GIVEN
    path = "./data/sample/betaflight/sample.bbl"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.BETAFLIGHT_BBL


def test_should_resolve_betaflight_bfl() -> None:
    # GIVEN
    path = "./data/sample/betaflight/sample.BFL"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.BETAFLIGHT_BFL
