"""Utilities for downloading and parsing uORB .msg files.

See references:
- https://docs.px4.io/main/en/msg_docs/
- https://px4.io/px4-parameters-part-1-overview/
- https://px4.io/px4-uorb-explained-part-4-ulog-flight-logging-system/
- https://github.com/PX4/PX4-Autopilot/tree/main/msg
"""

import functools
import pathlib
import shutil
from typing import Final

import git
import lark

from src import artifacts

REPO_NAME: Final[str] = "PX4-Autopilot"
REPO_URL: Final[str] = f"https://github.com/PX4/{REPO_NAME}"
DEFAULT_BRANCH: Final[str] = "main"


@functools.lru_cache(maxsize=16)
def git_clone(commit_sha: str | None, overwrite: bool) -> git.Repo:
    """Clone the PX4-Autopilot repository.

    Args:
        commit_sha (str | None): The specific commit SHA to checkout, or None to use the
            default branch.
        overwrite (bool): Whether to overwrite the local repo copy if it exists.

    Returns:
        git.Repo: The cloned git repository.

    """
    artifacts.git_clone_directory().mkdir(parents=True, exist_ok=True)
    repo_dir = artifacts.git_clone_directory() / REPO_NAME
    if repo_dir.exists() and not overwrite:
        repo = git.Repo(repo_dir)
    else:
        if repo_dir.exists():
            shutil.rmtree(repo_dir)
        repo = git.Repo.clone_from(REPO_URL, repo_dir)

    repo.git.fetch()
    if commit_sha is None:
        repo.git.checkout(DEFAULT_BRANCH)
        repo.git.pull("origin", DEFAULT_BRANCH)

    return repo


@functools.lru_cache(maxsize=256)
def msg_definition(type_name: str, commit_sha: str | None, overwrite: bool) -> str | None:
    """Return the uORB message definition (.msg) file content for the given message type."""
    repo = git_clone(commit_sha, overwrite)

    file_stem = "".join([word.capitalize() for word in type_name.split("_")])
    file_name = f"{file_stem}.msg"
    for path in [f"msg/{file_name}", f"msg/versioned/{file_name}"]:
        try:
            blob = repo.commit(commit_sha or DEFAULT_BRANCH).tree / path
            return blob.data_stream.read().decode("utf-8")
        except KeyError:
            pass

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
