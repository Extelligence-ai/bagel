"""Tests for the cloudini DecodePointCloudTask."""

import pathlib
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.tasks.cloudini.decode_pointcloud import (
    DecodePointCloudTask,
    _cloudini_available,
)

WASM_PATH = "/tmp/cloudini.wasm"  # noqa: S108


def test_empty_topics_raises() -> None:
    with pytest.raises(ValueError, match="at least one"):
        DecodePointCloudTask(
            topics=[], output_directory="/tmp/out", wasm_path=WASM_PATH  # noqa: S108
        )


def test_yaml_opt_out_skips_execution(tmp_path: pathlib.Path) -> None:
    task = DecodePointCloudTask(
        topics=["/lidar/points"],
        output_directory=str(tmp_path),
        wasm_path=WASM_PATH,
        cloudini=False,
    )
    task.execute(asof_seconds=1.0, lookback=None)
    assert not any(tmp_path.iterdir())


@patch("src.pipeline.tasks.cloudini.decode_pointcloud._HAS_CLOUDINI", False)
def test_missing_package_reports_unavailable() -> None:
    assert not _cloudini_available()


@patch("src.pipeline.tasks.cloudini.decode_pointcloud._HAS_CLOUDINI", True)
@patch("src.pipeline.tasks.cloudini.decode_pointcloud.settings")
def test_global_flag_disabled(mock_settings: MagicMock) -> None:
    mock_settings.CLOUDINI_ENABLED = False
    assert not _cloudini_available()


@patch("src.pipeline.tasks.cloudini.decode_pointcloud._HAS_CLOUDINI", True)
@patch("src.pipeline.tasks.cloudini.decode_pointcloud.settings")
def test_global_flag_enabled(mock_settings: MagicMock) -> None:
    mock_settings.CLOUDINI_ENABLED = True
    assert _cloudini_available()


def test_extract_raw_data_from_dict() -> None:
    task = DecodePointCloudTask(
        topics=["/lidar"], output_directory="/tmp/out", wasm_path=WASM_PATH  # noqa: S108
    )
    assert task._extract_raw_data({"data": b"\x01\x02"}) == b"\x01\x02"
    assert task._extract_raw_data({"other": 1}) is None


def test_extract_raw_data_from_object() -> None:
    task = DecodePointCloudTask(
        topics=["/lidar"], output_directory="/tmp/out", wasm_path=WASM_PATH  # noqa: S108
    )
    msg = MagicMock()
    msg.data = b"\x03\x04"
    assert task._extract_raw_data(msg) == b"\x03\x04"


def test_extract_raw_data_from_list() -> None:
    task = DecodePointCloudTask(
        topics=["/lidar"], output_directory="/tmp/out", wasm_path=WASM_PATH  # noqa: S108
    )
    msg = MagicMock()
    msg.data = [1, 2, 3]
    assert task._extract_raw_data(msg) == bytes([1, 2, 3])


@patch("src.pipeline.tasks.cloudini.decode_pointcloud._cloudini_available", return_value=True)
@patch("src.pipeline.tasks.cloudini.decode_pointcloud._create_decoder")
def test_execute_decodes_and_writes_npz(
    mock_create_decoder: MagicMock, mock_available: MagicMock, tmp_path: pathlib.Path
) -> None:
    import numpy as np

    mock_decoder = MagicMock()
    mock_create_decoder.return_value = mock_decoder

    point_cloud = np.array(
        [(1.0, 2.0, 3.0), (4.0, 5.0, 6.0)],
        dtype=[("x", "f4"), ("y", "f4"), ("z", "f4")],
    )
    mock_decoder.decode_message.return_value = (point_cloud, {"width": 2, "height": 1})

    task = DecodePointCloudTask(
        topics=["/lidar/points"],
        output_directory=str(tmp_path),
        wasm_path=WASM_PATH,
        output_format="npz",
    )

    mock_dataset = MagicMock()
    mock_dataset._messages.return_value = iter([
        ("/lidar/points", 1.0, {"data": b"\x00\x01"}),
    ])
    task._factory = MagicMock()
    task._registry = MagicMock()
    task._dataset = mock_dataset

    task.execute(asof_seconds=2.0, lookback=None)

    mock_decoder.decode_message.assert_called_once_with(b"\x00\x01")
    npz_files = list(tmp_path.rglob("*.npz"))
    assert len(npz_files) == 1


@patch("src.pipeline.tasks.cloudini.decode_pointcloud._cloudini_available", return_value=True)
@patch("src.pipeline.tasks.cloudini.decode_pointcloud._create_decoder")
def test_execute_decodes_and_writes_csv(
    mock_create_decoder: MagicMock, mock_available: MagicMock, tmp_path: pathlib.Path
) -> None:
    import numpy as np

    mock_decoder = MagicMock()
    mock_create_decoder.return_value = mock_decoder

    point_cloud = np.array(
        [(1.0, 2.0, 3.0)],
        dtype=[("x", "f4"), ("y", "f4"), ("z", "f4")],
    )
    mock_decoder.decode_message.return_value = (point_cloud, {"width": 1, "height": 1})

    task = DecodePointCloudTask(
        topics=["/lidar/points"],
        output_directory=str(tmp_path),
        wasm_path=WASM_PATH,
        output_format="csv",
    )

    mock_dataset = MagicMock()
    mock_dataset._messages.return_value = iter([
        ("/lidar/points", 0.5, MagicMock(data=b"\xAB\xCD")),
    ])
    task._factory = MagicMock()
    task._registry = MagicMock()
    task._dataset = mock_dataset

    task.execute(asof_seconds=1.0, lookback=None)

    csv_files = list(tmp_path.rglob("*.csv"))
    assert len(csv_files) == 1


def test_register() -> None:
    from src.di.module import global_registry
    from src.pipeline.tasks.cloudini.decode_pointcloud import register

    register()
    assert "src.pipeline.tasks.cloudini.decode_pointcloud" in global_registry
