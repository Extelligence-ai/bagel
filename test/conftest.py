"""Keep tests independent of user caches, capabilities, and output directories."""

import pathlib

import pytest

from settings import settings


@pytest.fixture(autouse=True)
def isolate_storage(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test owns its storage; cache lifecycle tests share it explicitly."""
    tmp_path = tmp_path / ".bagel-test"
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path / "cache"))
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    monkeypatch.setattr(settings, "USER_CAPABILITIES_DIRECTORY", str(tmp_path / "capabilities"))
