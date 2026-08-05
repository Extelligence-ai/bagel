"""Skills must be thin triggers: valid frontmatter, and every POML route real.

Guards the spec's core rule (skills route, POML owns the workflow): if a skill
references a capability path that stops existing, this fails at CI time instead
of at a user's session.
"""

import pathlib
import re

SKILL_FILES = sorted(pathlib.Path("plugin/skills").glob("*/SKILL.md"))
POML_ROUTE_PATTERN = re.compile(r"src/agent/[\w/]+\.poml")


def test_exactly_four_skills_exist() -> None:
    assert len(SKILL_FILES) == 4


def test_every_skill_has_frontmatter_name_and_description() -> None:
    for skill_file in SKILL_FILES:
        text = skill_file.read_text(encoding="utf-8")
        assert text.startswith("---\n"), skill_file
        frontmatter = text.split("---", 2)[1]
        assert re.search(r"^name: \S+", frontmatter, re.M), skill_file
        assert re.search(r"^description: .{20,}", frontmatter, re.M), skill_file


def test_every_poml_route_in_skills_exists_on_disk() -> None:
    routed = set()
    for skill_file in SKILL_FILES:
        routed.update(POML_ROUTE_PATTERN.findall(skill_file.read_text(encoding="utf-8")))
    assert routed, "expected at least one POML route across the skills"
    for route in routed:
        assert pathlib.Path(route).exists(), route


def test_referenced_reference_files_exist() -> None:
    for skill_file in SKILL_FILES:
        text = skill_file.read_text(encoding="utf-8")
        # Extract references from both forms:
        # ${CLAUDE_PLUGIN_ROOT}/references/...md and bare references/...md
        plugin_root_refs = re.findall(
            r"\$\{CLAUDE_PLUGIN_ROOT\}/(references/[\w-]+\.md)", text
        )
        bare_refs = re.findall(
            r"(?<!\$\{CLAUDE_PLUGIN_ROOT\}/)references/[\w-]+\.md", text
        )

        # Assert all referenced files exist
        for reference in plugin_root_refs + bare_refs:
            assert (pathlib.Path("plugin") / reference).exists(), reference

        # Assert no bare-path form remains (all must use ${CLAUDE_PLUGIN_ROOT}/)
        assert not bare_refs, (
            f"Bare 'references/...' paths found in {skill_file.name} "
            f"(must use ${{CLAUDE_PLUGIN_ROOT}}/): {bare_refs}"
        )
