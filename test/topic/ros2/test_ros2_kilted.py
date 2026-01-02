import pathlib

import lark


def test_lark_can_parse_iron_msg() -> None:
    """Only check if Lark can parse .msg files. No guarantee on content correctness."""
    with open(
        pathlib.Path(__file__).parent.parent.parent.parent / "src/topic/ros2/ros2msg/grammar.lark"
    ) as f:
        grammar = f.read()

    parser = lark.Lark(grammar, parser="earley")

    for path in (pathlib.Path(__file__).parent / "data/kilted/").rglob("*.msg"):
        with open(path) as f:
            parser.parse(f.read() + "\n")
