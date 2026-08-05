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
import tempfile

from settings import settings

_AGENT_ROOT = pathlib.Path(__file__).parent

_TASK_PATTERN = re.compile(r"<task>(.*?)</task>", re.DOTALL)
_TAG_PATTERN = re.compile(r"<[^>]+>")
_NAME_PATTERN = re.compile(r"^[a-z0-9_-]+(?:/[a-z0-9_-]+)?$")


class InvalidCapabilityError(Exception):
    """Raised when a capability cannot be saved or parameterized as requested."""


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


def _validate_poml_renders(content: str) -> None:
    """Reject content that poml() cannot render, before anything is saved."""
    from poml import poml  # deferred: keep module import light for discovery-only callers

    with tempfile.NamedTemporaryFile("w", suffix=".poml", encoding="utf-8", delete=False) as handle:
        handle.write(content)
        temp_path = pathlib.Path(handle.name)
    try:
        poml(str(temp_path))
    except Exception as exc:
        # poml's renderer raises implementation-defined exceptions for bad
        # markup; translate to the layer's typed error (#154 idiom).
        raise InvalidCapabilityError(
            f"Capability content does not render as POML: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        temp_path.unlink(missing_ok=True)


def save_capability(name: str, content: str, overwrite: bool = False) -> dict[str, str]:
    """Save a user-authored capability and return its discovery entry.

    Content starting with ``<poml`` is render-validated and saved as
    ``.poml``; any other non-empty content is saved as markdown. Writes are
    confined to ``settings.USER_CAPABILITIES_DIRECTORY``; builtins cannot be
    modified through this function.

    Raises:
        InvalidCapabilityError: On an invalid name, empty content,
            non-rendering POML, or a collision without ``overwrite=True``.

    """
    if not _NAME_PATTERN.fullmatch(name):
        raise InvalidCapabilityError(
            f"Invalid capability name {name!r}: use lowercase letters, digits, '-' or '_', "
            "with at most one '/' subdirectory level. The server chooses the file extension."
        )
    stripped = content.strip()
    if not stripped:
        raise InvalidCapabilityError("Capability content is empty.")
    is_poml = stripped.startswith("<poml")
    if is_poml:
        _validate_poml_renders(content)

    user_root = _user_root()
    target = user_root / f"{name}{'.poml' if is_poml else '.md'}"
    existing = [
        candidate
        for candidate in (user_root / f"{name}.poml", user_root / f"{name}.md")
        if candidate.exists()
    ]
    if existing and not overwrite:
        raise InvalidCapabilityError(
            f"Capability {name!r} already exists ({existing[0].name}); "
            "pass overwrite=True to replace it."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    for stale in existing:
        if stale != target:
            stale.unlink(missing_ok=True)
    target.write_text(content, encoding="utf-8")

    summarize = _summary if is_poml else _markdown_summary
    relative = target.relative_to(user_root)
    return {
        "name": f"user/{relative.with_suffix('')}",
        "path": str(target.resolve()),
        "summary": summarize(content, relative.stem),
    }
