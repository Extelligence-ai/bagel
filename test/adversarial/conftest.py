"""Shared configuration for adversarial tests."""

import pathlib

import pytest

collect_ignore_glob: list[str] = []


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark every test in this package as adversarial."""
    for item in items:
        if pathlib.Path(__file__).parent in item.path.parents:
            item.add_marker(pytest.mark.adversarial)
