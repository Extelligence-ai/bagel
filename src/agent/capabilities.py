"""Discover and author the capability files available to this server.

Backs the ``list_agent_capabilities`` MCP tool: walks ``src/agent/**/*.poml``
and reports each file's path and a one-line summary (the first sentence of its
``<task>`` element), so an LLM can discover and run capabilities via
``run_poml_capability`` without knowing any path in advance. Mirrors the
filesystem-walk approach of ``src/pipeline/capabilities.py``. User-authored
capabilities — .poml or .md files under ``settings.USER_CAPABILITIES_DIRECTORY``
— are discovered alongside the builtins with a ``user/`` name prefix.
"""

import pathlib
import re

from settings import settings

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


def _user_root() -> pathlib.Path:
    """Return the user-capabilities directory, read live so tests can monkeypatch it."""
    return pathlib.Path(settings.USER_CAPABILITIES_DIRECTORY)


def _markdown_summary(md_text: str, fallback: str) -> str:
    """First sentence of the body skipping headings, else the first H1, else fallback."""
    heading = ""
    for line in md_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            if not heading:
                heading = stripped.lstrip("#").strip()
            continue
        return stripped.split(". ", 1)[0].rstrip(".") + "."
    text = heading or fallback
    return text.split(". ", 1)[0].rstrip(".") + "."


def list_capabilities() -> list[dict[str, str]]:
    """Return every POML and user-authored capability with path and summary.

    Returns:
        list[dict[str, str]]: One dict per ``.poml`` or ``.md`` file, sorted by ``name``:
            ``name`` (relative path without suffix, e.g. ``compose/pipeline`` for builtins,
            ``user/battery-triage`` for user entries), ``path`` (repo-relative for builtins,
            absolute for user entries), and ``summary`` (first sentence of the ``<task>``
            element or first body paragraph for markdown).

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

    user_root = _user_root()
    if user_root.is_dir():
        for pattern in ("*.poml", "*.md"):
            for user_file in user_root.rglob(pattern):
                try:
                    text = user_file.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    continue  # vanished/unreadable mid-walk: skip, never crash discovery
                relative = user_file.relative_to(user_root)
                summarize = _summary if user_file.suffix == ".poml" else _markdown_summary
                capabilities.append(
                    {
                        "name": f"user/{relative.with_suffix('')}",
                        "path": str(user_file.resolve()),
                        "summary": summarize(text, relative.stem),
                    }
                )

    capabilities.sort(key=lambda capability: capability["name"])
    return capabilities
