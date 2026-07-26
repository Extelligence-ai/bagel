"""Tests for the external-fixture locator."""

import os
import pathlib

import pytest

from test._fixtures import external


def test_missing_env_reports_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BAGEL_EXTERNAL_FIXTURES", raising=False)
    assert external.griddle_recordings() == []
