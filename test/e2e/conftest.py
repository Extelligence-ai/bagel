"""Mark every test in the e2e package."""

import pathlib

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if pathlib.Path(__file__).parent in item.path.parents:
            item.add_marker(pytest.mark.e2e)
