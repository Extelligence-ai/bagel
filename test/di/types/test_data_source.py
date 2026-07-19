import os
import tempfile

import pytest

from bagel.di.types import data_source


def test_should_raise_for_unsupported_url_scheme() -> None:
    # GIVEN
    path = "http://localhost:9092"

    # WHEN / THEN
    with pytest.raises(NotImplementedError, match="URL scheme 'http' is not supported"):
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


def test_should_resolve_mcap_directory_as_first_class_mcap() -> None:
    # GIVEN
    path = "./data/sample/ros2/mcap"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.MCAP


def test_should_resolve_mcap_file_as_first_class_mcap() -> None:
    # GIVEN
    path = "./data/sample/ros2/mcap/part_0.mcap"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.MCAP


def test_should_resolve_mcap_zstd_file_as_first_class_mcap() -> None:
    # GIVEN
    path = "./data/sample/ros2/mcap_zstd/part_0.mcap.zstd"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.MCAP


def test_should_resolve_mcap_zstd_directory_as_first_class_mcap() -> None:
    # GIVEN
    path = "./data/sample/ros2/mcap_zstd"

    # WHEN
    result = data_source.resolve(path)

    # THEN
    assert result == data_source.DataSource.MCAP


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


# Edge-case robustness tests — addressing #134 (harden data source detection)


def test_has_magic_bytes_should_return_false_for_fifo() -> None:
    # GIVEN
    import os
    import pathlib

    with tempfile.TemporaryDirectory() as tmpdir:
        fifo_path = pathlib.Path(tmpdir) / "test.fifo"
        os.mkfifo(str(fifo_path))

        # WHEN
        result = data_source.has_magic_bytes(fifo_path, b"#ROSBAG V2")

        # THEN
        assert result is False


def test_has_magic_bytes_should_return_false_for_symlink_to_nonexistent() -> None:
    # GIVEN
    import pathlib

    with tempfile.TemporaryDirectory() as tmpdir:
        symlink_path = pathlib.Path(tmpdir) / "broken.link"
        symlink_path.symlink_to("/nonexistent/target")

        # WHEN
        result = data_source.has_magic_bytes(symlink_path, b"#ROSBAG V2")

        # THEN
        assert result is False


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root bypasses file permission checks, so a 0o000 file is still readable",
)
def test_has_magic_bytes_should_return_false_for_permission_denied() -> None:
    # GIVEN
    import pathlib

    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        tmpfile.write(b"#ROSBAG V2")
        tmpfile.flush()
        path = pathlib.Path(tmpfile.name)
        os.chmod(path, 0o000)

        try:
            # WHEN
            result = data_source.has_magic_bytes(path, b"#ROSBAG V2")

            # THEN
            assert result is False
        finally:
            os.chmod(path, 0o644)
            os.unlink(path)


def test_resolve_should_raise_for_fifo() -> None:
    # GIVEN
    import os
    import pathlib

    with tempfile.TemporaryDirectory() as tmpdir:
        fifo_path = pathlib.Path(tmpdir) / "test.fifo"
        os.mkfifo(str(fifo_path))

        # WHEN / THEN
        with pytest.raises(ValueError, match="Cannot resolve data source type from path:"):
            data_source.resolve(str(fifo_path))
