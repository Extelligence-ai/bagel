import pathlib
import shutil
import tempfile

import pytest

from src.source import errors
from src.source.ros2 import mcap


def test_source_factory() -> None:
    # GIVEN
    factory = mcap.SourceFactory("data/sample/ros2/mcap/")

    # WHEN
    metadata = factory.metadata

    # THEN
    assert metadata["version"] == "7"
    assert metadata["storage_identifier"] == "mcap"
    assert metadata["compression_format"] == ""
    assert metadata["compression_mode"] == ""
    assert metadata["relative_file_paths"] == ["part_0.mcap"]
    assert metadata["file_information"] == [
        {
            "duration_seconds": 2.970267,
            "message_count": 15,
            "path": "part_0.mcap",
            "start_time_seconds": 1689969664.041739,
        }
    ]


def test_validate_path_should_raise() -> None:
    # GIVEN
    path = pathlib.Path("data/sample/ros2/mcap/")

    # WHEN / THEN
    with tempfile.TemporaryDirectory() as tmpdir, pytest.raises(errors.MissingFilesError):
        shutil.copyfile(path / "metadata.yaml", pathlib.Path(tmpdir) / "metadata.yaml")
        mcap.SourceFactory(tmpdir).validate_path()
