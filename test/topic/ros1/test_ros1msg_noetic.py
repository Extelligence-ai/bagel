import pathlib

import lark


def test_lark_can_parse_noetic_msg() -> None:
    """Only check if Lark can parse .msg files. No guarantee on content correctness."""
    with open(
        pathlib.Path(__file__).parent.parent.parent.parent / "src/topic/ros1/grammar.lark"
    ) as f:
        grammar = f.read()

    parser = lark.Lark(grammar, parser="earley")

    for path in (pathlib.Path(__file__).parent / "data/noetic/").rglob("*.msg"):
        content = path.read_text(encoding="utf-8")
        parser.parse(content + "\n")
