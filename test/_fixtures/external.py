"""Locate optional external log fixtures (recordings kept outside this repo).

Tests reach external recordings via ``BAGEL_EXTERNAL_FIXTURES`` and skip
cleanly when it is unset or the path is missing — present locally, absent in CI.
"""

import os
import pathlib

import pytest

ENV_VAR = "BAGEL_EXTERNAL_FIXTURES"


def _root() -> pathlib.Path | None:
    raw = os.environ.get(ENV_VAR)
    if not raw:
        return None
    root = pathlib.Path(raw).expanduser()
    return root if root.is_dir() else None


def griddle_recordings() -> list[pathlib.Path]:
    """Return the external ``.mcap`` recordings, or an empty list if unavailable."""
    root = _root()
    if root is None:
        return []
    return sorted(root.glob("*.mcap"))


def require_external() -> pathlib.Path:
    """Return the external fixtures root, or skip the test if unavailable."""
    root = _root()
    if root is None:
        pytest.skip(f"{ENV_VAR} unset or path missing; external fixtures unavailable")
    return root
