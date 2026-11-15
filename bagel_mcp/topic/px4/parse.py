"""Utilities for downloading and parsing uORB .msg files.

See references:
- https://docs.px4.io/main/en/msg_docs/
- https://px4.io/px4-parameters-part-1-overview/
- https://px4.io/px4-uorb-explained-part-4-ulog-flight-logging-system/
- https://github.com/PX4/PX4-Autopilot/tree/main/msg
"""

from __future__ import annotations

import functools
import logging
import pathlib
import re
import urllib.error
import urllib.request
from typing import Final

import lark

from bagel_mcp import artifacts

REPO_NAME: Final[str] = "PX4-Autopilot"
REPO_URL: Final[str] = f"https://github.com/PX4/{REPO_NAME}"
RAW_URL: Final[str] = f"https://raw.githubusercontent.com/PX4/{REPO_NAME}"
DEFAULT_BRANCH: Final[str] = "main"
FETCH_TIMEOUT_SECONDS: Final[float] = 5.0
_MISSING: Final[str] = ".missing"
_REF = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

# Set after the first network failure, so an offline machine pays one timeout per
# process instead of one per message type.
_offline = False


def _candidate_paths(type_name: str) -> list[str]:
    """Return where the .msg file may live, newest layout first.

    PX4 >= 1.14 names files in CamelCase (some under msg/versioned/); older releases
    use the snake_case type name.
    """
    camel = "".join(word.capitalize() for word in type_name.split("_"))
    return [f"msg/{camel}.msg", f"msg/versioned/{camel}.msg", f"msg/{type_name}.msg"]


def _fetch(ref: str, path: str) -> str | None:
    """Return a file's content at a ref, None if it doesn't exist there."""
    global _offline  # noqa: PLW0603 -- process-wide circuit breaker
    if _offline:
        return None
    url = f"{RAW_URL}/{ref}/{path}"
    try:
        with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as response:  # noqa: S310 -- fixed https host
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        if error.code == 404:  # noqa: PLR2004
            return None
        raise
    except (urllib.error.URLError, TimeoutError):
        _offline = True
        raise


def msg_definition(type_name: str, commit_sha: str | None, overwrite: bool) -> str | None:
    """Return the uORB message definition (.msg) file content for the given message type.

    Fetches only that file from the PX4-Autopilot repository at the log's commit (or
    the default branch), and caches it on disk, found or not, so each type is fetched
    once per commit.
    """
    ref = commit_sha or DEFAULT_BRANCH
    if not _REF.match(ref) or not re.match(r"^\w+$", type_name):
        return None
    cache = artifacts.git_clone_directory() / "px4-msg" / ref
    cached, missing = cache / f"{type_name}.msg", cache / f"{type_name}{_MISSING}"
    if not overwrite:
        if cached.exists():
            return cached.read_text(encoding="utf-8")
        if missing.exists():
            return None
    for path in _candidate_paths(type_name):
        content = _fetch(ref, path)
        if content is not None:
            cache.mkdir(parents=True, exist_ok=True)
            cached.write_text(content, encoding="utf-8")
            return content
    cache.mkdir(parents=True, exist_ok=True)
    missing.touch()
    logging.info("No .msg for %s at %s", type_name, ref)
    return None


@functools.lru_cache(maxsize=256)
def descriptions(content: str) -> tuple[dict[str, str], dict[str, str]]:
    """Parse the .msg content and return the field and constant descriptions."""
    with open(pathlib.Path(__file__).parent / "grammar.lark") as f:
        grammar = f.read()
    parser = lark.Lark(grammar, parser="earley")
    lines = parser.parse(content + "\n").children

    def extract(line: lark.tree.Tree, token_type: str) -> str | None:
        match [t for t in line.children if t.type == token_type]:
            case []:
                return None
            case [t]:
                return t.value
            case _:
                raise ValueError(f"Multiple {token_type} tokens found in line: {line}")

    field_descriptions = {
        extract(line, "FIELD_NAME"): extract(line, "COMMENT").lstrip("# ").rstrip()
        for line in lines
        if line.data == "field"
        if extract(line, "COMMENT")
    }

    constant_descriptions = {
        extract(line, "CONSTANT_NAME"): extract(line, "COMMENT").lstrip("# ").rstrip()
        for line in lines
        if line.data == "constant"
        if extract(line, "COMMENT")
    }

    return field_descriptions, constant_descriptions
