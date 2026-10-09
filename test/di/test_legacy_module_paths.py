"""The package was `src` before 2.5.0; saved configs written against it must keep working."""

import logging

import pytest

from bagel_mcp.di import module
from bagel_mcp.pipeline import base


def test_canonical_translates_the_legacy_prefix(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        assert module.canonical("src.pipeline.tasks.reduce.mcap") == (
            "bagel_mcp.pipeline.tasks.reduce.mcap"
        )
    assert "deprecated" in caplog.text


def test_canonical_leaves_current_and_unrelated_paths_alone() -> None:
    assert module.canonical("bagel_mcp.source.mcap") == "bagel_mcp.source.mcap"
    # Only the package prefix is rewritten: a module that merely starts with
    # the letters "src" is somebody else's.
    assert module.canonical("srcery.tasks") == "srcery.tasks"


def test_provide_accepts_a_legacy_path() -> None:
    factory = module.provide("src.source.mcap", {"path": "./data/sample/ros2/mcap"})
    assert type(factory) is module.global_registry["bagel_mcp.source.mcap"]


def test_pipeline_config_accepts_a_legacy_module_key() -> None:
    config = {
        "name": "legacy_module_key",
        "site": "test_site",
        "asset": "test_asset",
        "path": "./data/sample/pyarrow/csv/flight.csv",
        "allow_failure": False,
        "cadence": {"topic": "message", "when": "once_at_end"},
        "tasks": [
            {
                "module": "src.pipeline.tasks.write_topics_to_file",
                "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
                "args": {"topics": ["message"], "output_format": "csv"},
            }
        ],
    }
    pipeline = base.Pipeline.build(config)
    (task,) = pipeline.tasks
    assert type(task) is module.global_registry["bagel_mcp.pipeline.tasks.write_topics_to_file"]
