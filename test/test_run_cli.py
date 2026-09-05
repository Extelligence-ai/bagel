"""Cover the pipeline CLI entrypoint without external services."""

import argparse
from pathlib import Path
from unittest.mock import Mock

import pytest

import run


def test_key_value_preserves_embedded_equals() -> None:
    assert run.parse_key_value("key=a=b") == ("key", "a=b")
    with pytest.raises(argparse.ArgumentTypeError):
        run.parse_key_value("invalid")


def test_missing_template_variable_is_actionable(tmp_path: Path) -> None:
    template = tmp_path / "template.yaml"
    template.write_text("path: {{ source }}")
    with pytest.raises(ValueError, match="Missing required template variable"):
        run.render_template(template, {})


def test_cli_builds_and_runs_rendered_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template = tmp_path / "template.yaml"
    template.write_text("path: '{{ source }}'")
    build = Mock()
    monkeypatch.setattr(run.base.Pipeline, "build", build)
    monkeypatch.setattr("sys.argv", ["run.py", str(template), "-v", "source=fixture.csv"])
    run.main()
    build.assert_called_once_with({"path": "fixture.csv"})
    build.return_value.run_all.assert_called_once_with()
