"""Discover and author the capability files available to this server.

Backs the ``list_agent_capabilities`` MCP tool: walks ``src/agent/**/*.poml``
and reports each file's path and a one-line summary (the first sentence of its
``<task>`` element), so an LLM can discover and run capabilities via
``run_poml_capability`` without knowing any path in advance. Mirrors the
filesystem-walk approach of ``src/pipeline/capabilities.py``. User-authored
capabilities — .poml or .md files under ``settings.USER_CAPABILITIES_DIRECTORY``
— are discovered alongside the builtins with a ``user/`` name prefix.
"""

import os
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


def _is_confined(path: pathlib.Path, root: pathlib.Path) -> bool:
    """Return True if ``path``'s resolved (symlink-followed) location is ``root`` or beneath it.

    The single containment check used by both save and discovery: a leaf-level
    ``.is_symlink()`` check alone misses an intermediate directory symlink (e.g.
    ``<root>/escape -> /outside/dir``, then operating on ``escape/whatever``),
    because resolving that path lands outside ``root`` even though no path
    *component* by itself looked suspicious. Resolving both sides and checking
    ``is_relative_to`` catches that regardless of which segment is the symlink.
    """
    try:
        return path.resolve().is_relative_to(root.resolve())
    except OSError:
        return False  # unresolvable (e.g. vanished mid-walk): treat as not confined


def _assert_confined(path: pathlib.Path, root: pathlib.Path, action: str) -> None:
    """Raise InvalidCapabilityError if ``path`` resolves outside ``root``."""
    if not _is_confined(path, root):
        raise InvalidCapabilityError(
            f"Refusing to {action} {path}: it resolves to {path.resolve()}, "
            f"outside the user-capabilities root {root.resolve()}."
        )


_LIST_MARKER_PATTERN = re.compile(r"^(?:[-*+]\s+|\d+[.)]\s+)")


def _markdown_summary(md_text: str, fallback: str) -> str:
    """First sentence of the body skipping headings, else the first heading, else fallback.

    Fence lines (```` ``` ````) and front-matter/horizontal-rule delimiters (``---``,
    ``***``) are skipped while scanning, as are lines inside a fenced block. Leading
    list markers (``- ``, ``* ``, ``+ ``, and numbered ``N. ``/``N) `` markers) are
    stripped from a candidate line before taking its first sentence, so a numbered-step
    body (e.g. ``"1. Open the bag. 2. Check topics."``) summarizes to its first step
    rather than to the literal ``"1."``. A candidate with no alphanumeric character is
    skipped rather than returned.
    """
    heading = ""
    in_fence = False
    for line in md_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if not stripped:
            continue
        if stripped in ("---", "***"):
            continue
        if stripped.startswith("#"):
            if not heading:
                heading = stripped.lstrip("#").strip()
            continue
        candidate = _LIST_MARKER_PATTERN.sub("", stripped, count=1)
        sentence = candidate.split(". ", 1)[0].rstrip(".") + "."
        if not any(character.isalnum() for character in sentence):
            continue
        return sentence
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
                if user_file.is_symlink():
                    continue  # never read through a symlink, planted or otherwise
                if not _is_confined(user_file, user_root):
                    continue  # reached via a symlinked ancestor directory instead
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


_ERROR_LINE_PATTERN = re.compile(r"^\w*Error:\s")


def _renderer_diagnostic(stderr: str) -> str:
    """Pull the useful line out of the POML Node renderer's stderr, if any.

    The renderer crashes with an uncaught Node exception, whose default dump is a
    source excerpt, then the actual ``SomeError: message`` line, then a long V8
    stack trace, then (for some error types) a dump of the exception object's
    extra properties, ending with a ``Node.js vX.Y.Z`` line. The real diagnostic
    therefore usually sits a few lines from the *start*, not the end, of stderr —
    grepping for the ``SomeError: `` line finds it directly. Falls back to the
    last few non-blank lines when no such line is found (e.g. plain text on
    stderr with no Node crash dump).
    """
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    for line in lines:
        if _ERROR_LINE_PATTERN.match(line):
            return line
    return "\n".join(lines[-3:])


def _validate_poml_renders(content: str) -> None:
    """Reject content that poml() cannot render, before anything is saved.

    poml() ignores the renderer subprocess's exit code and return value, so on
    failure it only ever sees an empty output file and raises a bare
    ``JSONDecodeError`` — the renderer's real diagnostic (e.g. "Component
    bogus-elem not found") goes to stderr and is otherwise lost. On failure we
    re-invoke the renderer ourselves via ``poml.cli.run(..., capture_output=True)``
    — its kwargs pass straight through to the underlying ``subprocess.run`` —
    solely to recover that stderr and fold it into the raised error.
    """
    from poml import poml  # deferred: keep module import light for discovery-only callers
    from poml.cli import run as poml_cli_run

    with tempfile.NamedTemporaryFile("w", suffix=".poml", encoding="utf-8", delete=False) as handle:
        handle.write(content)
        temp_path = pathlib.Path(handle.name)
    try:
        poml(str(temp_path))
    except Exception as exc:
        # poml's renderer raises implementation-defined exceptions for bad
        # markup; translate to the layer's typed error (#154 idiom).
        diagnostic = ""
        try:
            with tempfile.NamedTemporaryFile("r", suffix=".json") as recapture_output:
                completed = poml_cli_run(
                    "-f",
                    str(temp_path),
                    "-o",
                    recapture_output.name,
                    "--chat",
                    "true",
                    capture_output=True,
                    text=True,
                )
            diagnostic = _renderer_diagnostic(completed.stderr or "")
        except Exception:
            diagnostic = ""  # best-effort recapture; never mask the original error
        message = f"Capability content does not render as POML: {type(exc).__name__}: {exc}"
        if diagnostic:
            message += f"\nRenderer diagnostic: {diagnostic}"
        else:
            message += " (the renderer's diagnostic may be in the server log)"
        raise InvalidCapabilityError(message) from exc
    finally:
        temp_path.unlink(missing_ok=True)


def _write_capability_file(
    target: pathlib.Path, existing: list[pathlib.Path], content: str, user_root: pathlib.Path
) -> None:
    """Create the target's parent dir, write it atomically, then drop stale siblings.

    Raised OS errors (e.g. a non-writable directory on a fresh Linux bind mount)
    are translated to the module's typed error with a pointer to the likely fix.
    After creating the parent dir, its resolved location is checked against
    ``user_root``: a pre-planted *directory* symlink one level up (e.g.
    ``<root>/escape -> /outside/dir``) would otherwise let a write land outside
    the confinement guarantee even though no single path component looked like
    a symlink from the target's own leaf-level check.
    """
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        _assert_confined(target.parent, user_root, action="save a capability under")
        # Write to a sibling temp file and rename into place so a failed write
        # (disk full, permissions) leaves the previous capability intact; only
        # then drop stale same-name siblings of the other format.
        fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
        tmp = pathlib.Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
            os.replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        for stale in existing:
            if stale != target:
                stale.unlink(missing_ok=True)
    except OSError as exc:
        raise InvalidCapabilityError(
            f"Could not write capability to {target.parent}: {exc}. On Linux, run "
            "`mkdir -p ~/.bagel/capabilities` once before starting the container so "
            "the mount is owned by you, not root."
        ) from exc


def save_capability(name: str, content: str, overwrite: bool = False) -> dict[str, str]:
    """Save a user-authored capability and return its discovery entry.

    Content starting with ``<poml`` is render-validated and saved as
    ``.poml``; any other non-empty content is saved as markdown. Writes are
    confined to ``settings.USER_CAPABILITIES_DIRECTORY``; builtins cannot be
    modified through this function.

    A single leading ``"user/"`` is stripped from ``name`` before validation,
    so a name copied verbatim from a ``list_capabilities()`` entry (which
    prefixes user-authored capabilities with ``user/``) round-trips onto the
    same file instead of nesting under ``user/user/...``. Only the prefix
    (i.e. ``"user/"`` followed by something) is stripped — ``name="user"``
    alone is unaffected and still saves as a normal ``user.md``/``user.poml``
    slug.

    Raises:
        InvalidCapabilityError: On an invalid name, empty content,
            non-rendering POML, a collision without ``overwrite=True``, an
            attempt to write through a symlinked file or a symlinked ancestor
            directory (resolved location outside the user-capabilities root),
            or an OS-level failure (e.g. a non-writable directory) while
            creating or writing the file.

    """
    if name.startswith("user/"):
        name = name[len("user/") :]
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
    candidates = (user_root / f"{name}.poml", user_root / f"{name}.md")
    for candidate in candidates:
        if candidate.is_symlink():
            raise InvalidCapabilityError(
                f"Refusing to save over {candidate}: it is a symlink. "
                "save_capability only writes plain files under the user-capabilities "
                "directory; remove the symlink first if this was intentional."
            )
    existing = [candidate for candidate in candidates if candidate.exists()]
    if existing and not overwrite:
        raise InvalidCapabilityError(
            f"Capability {name!r} already exists ({existing[0].name}); "
            "pass overwrite=True to replace it."
        )
    _write_capability_file(target, existing, content, user_root)

    summarize = _summary if is_poml else _markdown_summary
    relative = target.relative_to(user_root)
    return {
        "name": f"user/{relative.with_suffix('')}",
        "path": str(target.resolve()),
        "summary": summarize(content, relative.stem),
    }
