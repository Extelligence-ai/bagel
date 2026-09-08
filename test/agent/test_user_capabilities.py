"""User-authored capabilities: discovery, markdown support, and saving.

Before this feature, list_capabilities() walked only the builtin src/agent
tree, so Docker users could not add capabilities without rebuilding the
image. Now a second root (settings.USER_CAPABILITIES_DIRECTORY) is walked,
accepting both .poml and .md files, with names prefixed "user/".
"""

import pathlib
from typing import NoReturn

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
    ["../escape", "/absolute", "a//b", "a/", ".hidden", "a/.dot", "", "name.poml", "Name.MD"],
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
    real_mkdir = pathlib.Path.mkdir

    def _raise_permission_error(self: pathlib.Path, *args: object, **kwargs: object) -> None:
        if self.is_relative_to(user_dir):
            raise PermissionError("Permission denied")
        real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "mkdir", _raise_permission_error)
    with pytest.raises(capabilities.InvalidCapabilityError, match="mkdir -p"):
        capabilities.save_capability("battery", "Body text.\n")


# --- Residual gap: directory-symlink escape (leaf-only checks missed this) -


def test_save_refuses_directory_symlink_escape(
    tmp_path: pathlib.Path, user_dir: pathlib.Path
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (user_dir / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(capabilities.InvalidCapabilityError, match="outside|escape|root"):
        capabilities.save_capability("escape/pwn", "Body text.\n")
    assert list(outside.iterdir()) == []


def test_directory_symlink_contents_not_discovered(
    tmp_path: pathlib.Path, user_dir: pathlib.Path
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "pwn.md").write_text("# Pwn\n\nSecret.\n", encoding="utf-8")
    (user_dir / "escape").symlink_to(outside, target_is_directory=True)
    assert "user/escape/pwn" not in _by_name()


def test_save_into_normal_subdirectory_still_works(user_dir: pathlib.Path) -> None:
    saved = capabilities.save_capability("fleet/battery2", "Check cells again.\n")
    assert (user_dir / "fleet" / "battery2.md").exists()
    assert saved["name"] == "user/fleet/battery2"


def test_failed_overwrite_preserves_previous_capability(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write failure mid-overwrite must not lose the last valid capability."""
    monkeypatch.setattr(settings, "USER_CAPABILITIES_DIRECTORY", str(tmp_path))
    capabilities.save_capability("keep-me", "# v1\n\nFirst version.")
    target = tmp_path / "keep-me.md"
    assert target.read_text(encoding="utf-8").startswith("# v1")

    def boom(*args: object, **kwargs: object) -> NoReturn:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(capabilities.os, "replace", boom)
    with pytest.raises(capabilities.InvalidCapabilityError):
        capabilities.save_capability("keep-me", "# v2\n\nSecond version.", overwrite=True)

    assert target.read_text(encoding="utf-8").startswith("# v1")
    assert [p.name for p in tmp_path.iterdir() if not p.name.startswith(".")] == ["keep-me.md"]


def test_deeply_nested_name_round_trips_through_discovery(user_dir: pathlib.Path) -> None:
    """Any depth discovery can emit, save must accept (Codex review)."""
    saved = capabilities.save_capability("fleet/ros2/check", "# Check\n\nNested.")
    assert saved["name"] == "user/fleet/ros2/check"
    assert (user_dir / "fleet" / "ros2" / "check.md").exists()
    names = [entry["name"] for entry in capabilities.list_capabilities()]
    assert "user/fleet/ros2/check" in names
    again = capabilities.save_capability("user/fleet/ros2/check", "# Check 2", overwrite=True)
    assert again["path"] == saved["path"]


def test_concurrent_saves_of_new_name_honor_no_overwrite(user_dir: pathlib.Path) -> None:
    """Only one of N overlapping overwrite=False saves may win (Codex review)."""
    import concurrent.futures

    def attempt(i: int) -> str:
        try:
            capabilities.save_capability("race", f"# v{i}\n\nbody")
            return "ok"
        except capabilities.InvalidCapabilityError:
            return "exists"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(attempt, range(8)))
    assert results.count("ok") == 1
    assert results.count("exists") == 7
    assert [p.name for p in user_dir.iterdir() if not p.name.startswith(".")] == ["race.md"]


def test_discovered_file_name_with_spaces_round_trips(user_dir: pathlib.Path) -> None:
    """Discovery emits whatever the file is called; save must accept it (Codex review)."""
    (user_dir / "Flight Checks.md").write_text("# Flight\n\nManual.", encoding="utf-8")
    names = [entry["name"] for entry in capabilities.list_capabilities()]
    assert "user/Flight Checks" in names
    saved = capabilities.save_capability("user/Flight Checks", "# Flight 2", overwrite=True)
    assert saved["path"] == str((user_dir / "Flight Checks.md").resolve())


def test_parameterized_poml_saves_without_context(user_dir: pathlib.Path) -> None:
    """A template with {{variables}} is valid POML; context arrives at run time."""
    doc = "<poml><task>Investigate {{topic_name}} over {{window_s}} seconds</task></poml>"
    saved = capabilities.save_capability("param-check", doc)
    assert saved["path"].endswith("param-check.poml")


# --- delete_capability: lifecycle round-trip, builtin refusal, traversal ---


def test_delete_capability_round_trip(user_dir: pathlib.Path) -> None:
    capabilities.save_capability("battery-triage", VALID_POML)
    assert "user/battery-triage" in _by_name()

    result = capabilities.delete_capability("user/battery-triage")
    assert result["name"] == "user/battery-triage"
    assert not (user_dir / "battery-triage.poml").exists()
    assert "user/battery-triage" not in _by_name()


def test_delete_capability_rejects_bare_name(user_dir: pathlib.Path) -> None:
    """Review #224 (Copilot): only the full `user/`-prefixed name is accepted --
    a bare slug is ambiguous once a user capability can shadow a builtin stem."""
    capabilities.save_capability("preflight", "# Preflight\n\nCheck the props.\n")
    with pytest.raises(capabilities.InvalidCapabilityError, match="full name"):
        capabilities.delete_capability("preflight")
    assert (user_dir / "preflight.md").exists()


def test_delete_capability_bare_name_does_not_delete_shadowed_builtin_stem(
    user_dir: pathlib.Path,
) -> None:
    """The shadowing case the review called out: compose/pipeline (builtin) and
    user/compose/pipeline (user copy) share a stem. A bare "compose/pipeline"
    must be refused -- and must not silently resolve to either file -- while
    the full `user/compose/pipeline` name deletes only the user copy."""
    (user_dir / "compose").mkdir()
    (user_dir / "compose" / "pipeline.poml").write_text(
        "<poml><task>My own pipeline recipe. Extra.</task></poml>", encoding="utf-8"
    )
    assert "compose/pipeline" in _by_name()  # builtin
    assert "user/compose/pipeline" in _by_name()  # user shadow

    with pytest.raises(capabilities.InvalidCapabilityError, match="builtin"):
        capabilities.delete_capability("compose/pipeline")
    assert (user_dir / "compose" / "pipeline.poml").exists()  # user copy untouched

    result = capabilities.delete_capability("user/compose/pipeline")
    assert result["name"] == "user/compose/pipeline"
    assert not (user_dir / "compose" / "pipeline.poml").exists()
    assert "compose/pipeline" in _by_name()  # builtin still present


def test_delete_capability_second_delete_raises(user_dir: pathlib.Path) -> None:
    capabilities.save_capability("battery-triage", VALID_POML)
    capabilities.delete_capability("user/battery-triage")
    with pytest.raises(capabilities.InvalidCapabilityError, match="battery-triage"):
        capabilities.delete_capability("user/battery-triage")


def test_delete_capability_unknown_name_lists_available(user_dir: pathlib.Path) -> None:
    capabilities.save_capability("battery-triage", VALID_POML)
    with pytest.raises(capabilities.InvalidCapabilityError, match="battery-triage"):
        capabilities.delete_capability("user/does-not-exist")


def test_delete_capability_refuses_builtin(user_dir: pathlib.Path) -> None:
    with pytest.raises(capabilities.InvalidCapabilityError, match="builtin"):
        capabilities.delete_capability("compose/pipeline")
    # the builtin file itself is untouched
    assert "compose/pipeline" in _by_name()


@pytest.mark.parametrize(
    "bad_name", ["user/../escape", "user//absolute", "user/a//b", "user/.hidden"]
)
def test_delete_capability_rejects_bad_names(user_dir: pathlib.Path, bad_name: str) -> None:
    with pytest.raises(capabilities.InvalidCapabilityError):
        capabilities.delete_capability(bad_name)


def test_delete_capability_confinement_checked_before_unlink(
    user_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A traversal-style name must never reach unlink(), regardless of which
    validation layer (prefix, syntax, or containment) catches it."""

    def boom(self: pathlib.Path) -> None:  # pragma: no cover - should never run
        raise AssertionError("unlink should not be called for a traversal name")

    monkeypatch.setattr(pathlib.Path, "unlink", boom)
    with pytest.raises(capabilities.InvalidCapabilityError):
        capabilities.delete_capability("user/../escape")


def test_delete_capability_refuses_directory_symlink_escape(
    tmp_path: pathlib.Path, user_dir: pathlib.Path
) -> None:
    """A name that passes syntax validation but escapes via a symlinked
    ancestor directory must still be caught by the containment check itself,
    not just the syntax guard (mirrors delete_pipeline's equivalent test)."""
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "pwn.md").write_text("# Pwn\n\nSecret.\n", encoding="utf-8")
    (user_dir / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(capabilities.InvalidCapabilityError, match="outside|escape|root"):
        capabilities.delete_capability("user/escape/pwn")
    assert (outside / "pwn.md").exists()


def test_delete_capability_serializes_with_save_lock(
    user_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copilot review: the exists-check + unlink must hold the same lock
    save_capability's existence-check + write does, so a concurrent save and
    delete of the same name cannot interleave. A spy proves the lock is held
    while unlink() runs."""
    capabilities.save_capability("battery-triage", VALID_POML)

    shared_lock = capabilities._save_lock(capabilities._user_root())
    monkeypatch.setattr(capabilities, "_save_lock", lambda root: shared_lock)

    seen: dict[str, bool] = {}
    real_unlink = pathlib.Path.unlink

    def spy_unlink(self: pathlib.Path, *args: object, **kwargs: object) -> None:
        seen["locked"] = shared_lock.is_locked
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "unlink", spy_unlink)
    capabilities.delete_capability("user/battery-triage")
    assert seen["locked"] is True


def test_delete_capability_tool_round_trip(user_dir: pathlib.Path) -> None:
    import server

    server.save_agent_capability("via-tool", VALID_POML)
    result = server.delete_capability("user/via-tool")
    assert result["name"] == "user/via-tool"
    names = {capability["name"] for capability in server.list_agent_capabilities()}
    assert "user/via-tool" not in names


def test_save_lock_is_not_inside_user_directory(
    user_dir: pathlib.Path, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A planted .save.lock symlink in the (git-synced) user dir must be inert."""
    cache = tmp_path / "cache"
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(cache))
    victim = tmp_path / "victim.txt"
    victim.write_text("precious", encoding="utf-8")
    (user_dir / ".save.lock").symlink_to(victim)
    capabilities.save_capability("safe", "# Safe\n\nbody")
    assert victim.read_text(encoding="utf-8") == "precious"
    assert (user_dir / "safe.md").exists()
    assert list((cache / "locks").glob("capabilities-*.lock"))
