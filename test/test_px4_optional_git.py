"""PX4's optional online descriptions must not prevent local ULog reads."""

import os
import pathlib
import subprocess
import sys

import pytest


@pytest.mark.parametrize("download_description", [False, True])
def test_px4_reads_without_a_git_executable(
    tmp_path: pathlib.Path, download_description: bool
) -> None:
    pytest.importorskip("pyulog")
    # A fresh interpreter exercises GitPython's import-time executable probe,
    # even when another test has already imported it successfully on this host.
    script = f"""
from bagel_mcp.source.px4.ulg import SourceFactory
from bagel_mcp.topic.px4.ulg import TopicRegistry
from bagel_mcp.message.px4.ulg import MessageDataset

source = SourceFactory('data/sample/px4/sample.ulg')
data = source.build()
registry = TopicRegistry(download_description={download_description!r})
topic = 'vehicle_imu_0'
assert registry.message_count(topic, data) == 83
assert registry.struct(topic, data).get_field_index('timestamp') >= 0
assert registry.describe(topic, data) is None
dataset = MessageDataset(use_cache=False)
assert dataset.to_duckdb(source, registry, topics=[topic]).shape == (83, 2)
"""
    result = subprocess.run(  # noqa: S603 -- fixed test program
        [sys.executable, "-c", script],
        env={
            **os.environ,
            "GIT_PYTHON_GIT_EXECUTABLE": str(tmp_path / "absent-git"),
            "GIT_PYTHON_REFRESH": "error",
            "CACHE_DIRECTORY": str(tmp_path / "cache"),
        },
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
