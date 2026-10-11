"""Skills carry their own workflow: valid frontmatter, no server-loaded instructions.

Anthropic's plugin directory requires every instruction Claude acts on to ship
inside the reviewed plugin. A skill that loads a POML capability from the server
and follows it would pull behavior from outside the plugin, so this fails at CI
time instead
of at a user's session.
"""

import pathlib
import re

SKILL_FILES = sorted(pathlib.Path("plugin/skills").glob("*/SKILL.md"))
POML_ROUTE_PATTERN = re.compile(r"bagel_mcp/agent/[\w/]+\.poml")


def test_exactly_four_skills_exist() -> None:
    assert len(SKILL_FILES) == 4


def test_every_skill_has_frontmatter_name_and_description() -> None:
    for skill_file in SKILL_FILES:
        text = skill_file.read_text(encoding="utf-8")
        assert text.startswith("---\n"), skill_file
        frontmatter = text.split("---", 2)[1]
        assert re.search(r"^name: \S+", frontmatter, re.M), skill_file
        assert re.search(r"^description: .{20,}", frontmatter, re.M), skill_file


def test_no_skill_loads_instructions_from_the_server() -> None:
    # Looking up data (list_pipeline_capabilities) is fine; loading a capability
    # or prompt from the server and following it is not.
    for skill_file in SKILL_FILES:
        text = skill_file.read_text(encoding="utf-8")
        assert not POML_ROUTE_PATTERN.search(text), skill_file
        assert "run_poml_capability" not in text, skill_file
        assert not re.search(r"follow (it|them) exactly", text, re.I), skill_file


def test_referenced_reference_files_exist() -> None:
    for skill_file in SKILL_FILES:
        text = skill_file.read_text(encoding="utf-8")
        for reference in re.findall(r"references/[\w-]+\.md", text):
            assert (pathlib.Path("plugin") / reference).exists(), reference


def test_skills_only_reference_real_mcp_tools() -> None:
    """Tool names drift; a skill naming a nonexistent tool strands the agent."""
    real = set(re.findall(r"^def (\w+)\(", pathlib.Path("bagel_mcp/server.py").read_text(), re.M))
    claimed = set()
    for skill_file in SKILL_FILES:
        claimed.update(re.findall(r"`(\w+)`", skill_file.read_text(encoding="utf-8")))
    fake_tools = {
        name
        for name in claimed
        if (
            name.startswith(
                (
                    "describe_",
                    "query_",
                    "list_",
                    "run_",
                    "preview_",
                    "save_",
                    "export_",
                    "subscribe_",
                    "read_",
                )
            )
            and name not in real
        )
    }
    assert not fake_tools, f"skills reference nonexistent tools: {sorted(fake_tools)}"
