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


VALID_POML = "<poml><task>Saved workflow. More detail.</task></poml>"


def test_save_poml_round_trip(user_dir: pathlib.Path) -> None:
    saved = capabilities.save_capability("battery-triage", VALID_POML)
    assert saved["name"] == "user/battery-triage"
    assert saved["path"].endswith("battery-triage.poml")
    assert saved["summary"] == "Saved workflow."
    assert "user/battery-triage" in _by_name()


def test_save_markdown_round_trip(user_dir: pathlib.Path) -> None:
    saved = capabilities.save_capability("preflight", "# Preflight\n\nCheck the props.\n")
    assert saved["path"].endswith("preflight.md")
    assert saved["summary"] == "Check the props."


def test_save_into_subdirectory(user_dir: pathlib.Path) -> None:
    saved = capabilities.save_capability("fleet/battery", "Check cells.\n")
    assert (user_dir / "fleet" / "battery.md").exists()
    assert saved["name"] == "user/fleet/battery"


@pytest.mark.parametrize(
    "bad_name",
    ["../escape", "/absolute", "Upper", "a b", "a/b/c", "", "name.poml"],
)
def test_save_rejects_bad_names(user_dir: pathlib.Path, bad_name: str) -> None:
    with pytest.raises(capabilities.InvalidCapabilityError):
        capabilities.save_capability(bad_name, "content")


def test_save_rejects_empty_content(user_dir: pathlib.Path) -> None:
    with pytest.raises(capabilities.InvalidCapabilityError, match="empty"):
        capabilities.save_capability("blank", "   \n")


def test_save_rejects_non_rendering_poml_and_writes_nothing(user_dir: pathlib.Path) -> None:
    with pytest.raises(capabilities.InvalidCapabilityError, match="render"):
        capabilities.save_capability("broken", "<poml><task>unclosed")
    assert list(user_dir.rglob("*")) == []


def test_save_collision_requires_overwrite(user_dir: pathlib.Path) -> None:
    capabilities.save_capability("dupe", "First.\n")
    with pytest.raises(capabilities.InvalidCapabilityError, match="overwrite=True"):
        capabilities.save_capability("dupe", "Second.\n")


def test_overwrite_replaces_across_formats(user_dir: pathlib.Path) -> None:
    capabilities.save_capability("swap", "Markdown first.\n")
    capabilities.save_capability("swap", VALID_POML, overwrite=True)
    assert (user_dir / "swap.poml").exists()
    assert not (user_dir / "swap.md").exists()


def test_run_markdown_capability_end_to_end(user_dir: pathlib.Path) -> None:
    import server

    saved = capabilities.save_capability("checklist", "# Checklist\n\nDo the thing.\n")
    result = server.run_poml_capability(saved["path"])
    assert isinstance(result, list) and len(result) == 1
    assert "Do the thing." in result[0]["content"]


def test_run_markdown_with_context_raises(user_dir: pathlib.Path) -> None:
    import server

    saved = capabilities.save_capability("static", "No variables here.\n")
    with pytest.raises(capabilities.InvalidCapabilityError, match="POML"):
        server.run_poml_capability(saved["path"], poml_context={"x": 1})


def test_save_tool_round_trip(user_dir: pathlib.Path) -> None:
    import server

    saved = server.save_agent_capability("via-tool", VALID_POML)
    assert saved["name"] == "user/via-tool"
    names = {capability["name"] for capability in server.list_agent_capabilities()}
    assert "user/via-tool" in names


# --- Item 1: user/ round-trip double-prefix -------------------------------


def test_save_with_discovered_user_prefix_overwrites_same_file(user_dir: pathlib.Path) -> None:
    capabilities.save_capability("battery", "First.\n")
    saved = capabilities.save_capability("user/battery", "Second.\n", overwrite=True)
    assert saved["name"] == "user/battery"
    assert (user_dir / "battery.md").exists()
    assert not (user_dir / "user").exists()
    assert (user_dir / "battery.md").read_text(encoding="utf-8") == "Second.\n"


def test_bare_user_name_is_a_normal_slug(user_dir: pathlib.Path) -> None:
    saved = capabilities.save_capability("user", "Just user.\n")
    assert saved["name"] == "user/user"
    assert (user_dir / "user.md").exists()
    assert not (user_dir / "user").exists()


# --- Item 2: _markdown_summary and numbered-step markdown ------------------


def test_markdown_summary_strips_numbered_list_marker(user_dir: pathlib.Path) -> None:
    (user_dir / "steps.md").write_text("1. Open the bag. 2. Check topics.\n", encoding="utf-8")
    assert _by_name()["user/steps"]["summary"] == "Open the bag."


def test_markdown_summary_skips_front_matter_delimiter(user_dir: pathlib.Path) -> None:
    (user_dir / "fm.md").write_text(
        "---\nCheck the props before flight. Then log it.\n", encoding="utf-8"
    )
    assert _by_name()["user/fm"]["summary"] == "Check the props before flight."


def test_markdown_summary_skips_leading_code_fence(user_dir: pathlib.Path) -> None:
    (user_dir / "fenced.md").write_text(
        "```\ncode here\n```\n\nRun the checklist twice. Then verify.\n",
        encoding="utf-8",
    )
    assert _by_name()["user/fenced"]["summary"] == "Run the checklist twice."


# --- Item 3: render-failure diagnostics ------------------------------------


def test_save_rejects_non_rendering_poml_with_diagnostic(user_dir: pathlib.Path) -> None:
    with pytest.raises(capabilities.InvalidCapabilityError) as exc_info:
        capabilities.save_capability(
            "broken-component",
            "<poml><bogus-elem>hi</bogus-elem><task>Do something.</task></poml>",
        )
    message = str(exc_info.value)
    assert "not found" in message.lower() or "server log" in message.lower()


# --- Item 4: symlink write-through and discovery ---------------------------


def test_save_refuses_symlink_write_through(tmp_path: pathlib.Path, user_dir: pathlib.Path) -> None:
    victim = tmp_path / "victim.md"
    victim.write_text("original\n", encoding="utf-8")
    (user_dir / "linked.md").symlink_to(pathlib.Path("../victim.md"))
    with pytest.raises(capabilities.InvalidCapabilityError, match="symlink"):
        capabilities.save_capability("linked", "New content.\n", overwrite=True)
    assert victim.read_text(encoding="utf-8") == "original\n"


def test_symlinked_capability_not_discovered(
    tmp_path: pathlib.Path, user_dir: pathlib.Path
) -> None:
    victim = tmp_path / "victim.md"
    victim.write_text("# Victim\n\nSecret content.\n", encoding="utf-8")
    (user_dir / "linked.md").symlink_to(pathlib.Path("../victim.md"))
    assert "user/linked" not in _by_name()


# --- Item 5: first-save PermissionError on Linux ---------------------------


def test_save_wraps_permission_error(
    user_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise_permission_error(self: pathlib.Path, *args: object, **kwargs: object) -> None:
        raise PermissionError("Permission denied")

    monkeypatch.setattr(pathlib.Path, "mkdir", _raise_permission_error)
    with pytest.raises(capabilities.InvalidCapabilityError, match="mkdir -p"):
        capabilities.save_capability("battery", "Body text.\n")
