"""Paths with a leading `~` work in every tool, the way a user types them.

Agents forward paths as given; on a pip install those are host paths, so
`~/logs/run.mcap` is the natural spelling. Without expansion, every reader
saw a literal `~` directory and reported the file missing.
"""

import ast
import pathlib
import shutil

import pytest

from bagel_mcp import server

SAMPLE = pathlib.Path("./data/sample/ros1/sample.bag")


def test_query_messages_accepts_a_home_relative_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # GIVEN a bag under the user's home, referred to with ~
    (tmp_path / "logs").mkdir()
    shutil.copy(SAMPLE, tmp_path / "logs" / "sample.bag")
    monkeypatch.setenv("HOME", str(tmp_path))

    # WHEN
    rows = server.query_messages(
        path="~/logs/sample.bag",
        sql_statement='SELECT COUNT(*) AS n FROM "/turtle1/cmd_vel"',
        topic="/turtle1/cmd_vel",
    )

    # THEN
    assert rows[0]["n"] > 0


def test_every_tool_with_a_path_expands_it() -> None:
    # Any new tool that takes a path must expand ~ too.
    tree = ast.parse(pathlib.Path(server.__file__).read_text())
    for fn in tree.body:
        if not isinstance(fn, ast.FunctionDef):
            continue
        is_tool = any(
            isinstance(d, ast.Call) and getattr(d.func, "attr", "") == "tool"
            for d in fn.decorator_list
        )
        params = {a.arg for a in fn.args.args + fn.args.kwonlyargs}
        for name in params & {"path", "paths", "poml_path"} if is_tool else set():
            assert f"expand_user({name})" in ast.unparse(fn) or (
                name == "paths" and "expand_user(p) for p in paths" in ast.unparse(fn)
            ), f"{fn.name} does not expand ~ in {name}"
