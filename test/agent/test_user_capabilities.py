"""User-authored capabilities: discovery, markdown support, and saving.

Before this feature, list_capabilities() walked only the builtin src/agent
tree, so Docker users could not add capabilities without rebuilding the
image. Now a second root (settings.USER_CAPABILITIES_DIRECTORY) is walked,
accepting both .poml and .md files, with names prefixed "user/".
"""

import pathlib

import pytest

from settings import settings
from src.agent import capabilities


@pytest.fixture
def user_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    directory = tmp_path / "capabilities"
    directory.mkdir()
    monkeypatch.setattr(settings, "USER_CAPABILITIES_DIRECTORY", str(directory))
    return directory


def _by_name() -> dict[str, dict[str, str]]:
    return {capability["name"]: capability for capability in capabilities.list_capabilities()}


def test_missing_user_dir_yields_builtins_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "USER_CAPABILITIES_DIRECTORY", "/nonexistent/bagel-capabilities")
    names = set(_by_name())
    assert "compose/pipeline" in names
    assert not any(name.startswith("user/") for name in names)


def test_user_poml_discovered_with_prefix_and_absolute_path(user_dir: pathlib.Path) -> None:
    (user_dir / "battery-triage.poml").write_text(
        "<poml><task>Check battery health after a flight. Then report.</task></poml>",
        encoding="utf-8",
    )
    entry = _by_name()["user/battery-triage"]
    assert pathlib.Path(entry["path"]).is_absolute()
    assert entry["summary"] == "Check battery health after a flight."


def test_user_markdown_discovered_and_summarized(user_dir: pathlib.Path) -> None:
    (user_dir / "preflight.md").write_text(
        "# Preflight check\n\nVerify props and battery before arming. Then log it.\n",
        encoding="utf-8",
    )
    entry = _by_name()["user/preflight"]
    assert entry["summary"] == "Verify props and battery before arming."


def test_markdown_summary_falls_back_to_h1_then_stem(user_dir: pathlib.Path) -> None:
    (user_dir / "only-heading.md").write_text("# Only A Heading\n", encoding="utf-8")
    (user_dir / "empty.md").write_text("", encoding="utf-8")
    by_name = _by_name()
    assert by_name["user/only-heading"]["summary"] == "Only A Heading."
    assert by_name["user/empty"]["summary"] == "empty."


def test_same_stem_as_builtin_does_not_collide(user_dir: pathlib.Path) -> None:
    (user_dir / "compose").mkdir()
    (user_dir / "compose" / "pipeline.poml").write_text(
        "<poml><task>My own pipeline recipe. Extra.</task></poml>", encoding="utf-8"
    )
    by_name = _by_name()
    assert "compose/pipeline" in by_name  # builtin untouched
    assert "user/compose/pipeline" in by_name  # user's version, distinct name


def test_nested_subdirectory_preserved_in_name(user_dir: pathlib.Path) -> None:
    (user_dir / "fleet").mkdir()
    (user_dir / "fleet" / "battery-triage.md").write_text("Check cells.\n", encoding="utf-8")
    assert "user/fleet/battery-triage" in _by_name()


def test_combined_list_sorted_by_name(user_dir: pathlib.Path) -> None:
    (user_dir / "aaa.md").write_text("First.\n", encoding="utf-8")
    names = [capability["name"] for capability in capabilities.list_capabilities()]
    assert names == sorted(names)
