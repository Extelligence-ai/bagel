"""Discover the POML capability files shipped under ``src/agent``.

Backs the ``list_agent_capabilities`` MCP tool: walks ``src/agent/**/*.poml``
and reports each file's path and a one-line summary (the first sentence of its
``<task>`` element), so an LLM can discover and run capabilities via
``run_poml_capability`` without knowing any path in advance. Mirrors the
filesystem-walk approach of ``src/pipeline/capabilities.py``.
"""

import pathlib
import re

_AGENT_ROOT = pathlib.Path(__file__).parent

_TASK_PATTERN = re.compile(r"<task>(.*?)</task>", re.DOTALL)
_TAG_PATTERN = re.compile(r"<[^>]+>")


def _summary(poml_text: str, fallback: str) -> str:
    """Return the first sentence of the file's <task> element, or the fallback."""
    match = _TASK_PATTERN.search(poml_text)
    text = ""
    if match:
        text = " ".join(_TAG_PATTERN.sub(" ", match.group(1)).split())
    if not text:
        text = fallback
    return text.split(". ", 1)[0].rstrip(".") + "."


def list_capabilities() -> list[dict[str, str]]:
    """Return every POML capability under ``src/agent`` with path and summary.

    Returns:
        list[dict[str, str]]: One dict per ``.poml`` file, sorted by ``name``:
            ``name`` (relative path without suffix, e.g. ``compose/pipeline``),
            ``path`` (repo-relative, ready for ``run_poml_capability``), and
            ``summary`` (first sentence of the ``<task>`` element).

    """
    capabilities = []
    for poml_file in _AGENT_ROOT.rglob("*.poml"):
        relative = poml_file.relative_to(_AGENT_ROOT)
        text = poml_file.read_text(encoding="utf-8", errors="replace")
        capabilities.append(
            {
                "name": str(relative.with_suffix("")),
                "path": f"./src/agent/{relative}",
                "summary": _summary(text, fallback=relative.stem),
            }
        )
    capabilities.sort(key=lambda capability: capability["name"])
    return capabilities
