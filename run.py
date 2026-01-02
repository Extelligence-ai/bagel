"""Run a Bagel pipeline based on a Jinja template."""

import argparse
import logging
import pathlib

import yaml
from jinja2 import StrictUndefined, Template, UndefinedError

from src.pipeline import base


def parse_key_value(arg: str) -> tuple[str, str]:
    """Parse a key=value argument."""
    if "=" not in arg:
        raise argparse.ArgumentTypeError(f"Invalid format: {arg}. Expected key=value")
    key, value = arg.split("=", 1)
    return key, value


def render_template(template_path: pathlib.Path, variables: dict[str, str]) -> str:
    """Render a Jinja template with the given variables."""
    with open(template_path) as f:
        template = Template(f.read(), undefined=StrictUndefined)

    try:
        return template.render(**variables)
    except UndefinedError as e:
        raise ValueError(f"Missing required template variable: {e}") from e


def main() -> None:
    """Run a Bagel pipeline based on a Jinja template."""
    parser = argparse.ArgumentParser(
        description="Run a Bagel pipeline",
        epilog="Example: %(prog)s template.yaml -v site=warehouse -v asset=forklift",
    )

    parser.add_argument("template", type=pathlib.Path, help="Path to the Jinja template file")

    parser.add_argument(
        "-v",
        "--var",
        action="append",
        type=parse_key_value,
        dest="variables",
        metavar="KEY=VALUE",
        help="Template variables in key=value format (can be used multiple times)",
    )

    parser.add_argument(
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
        default=logging.WARNING,
        help="Print INFO level statements",
    )

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    # Render template with variables
    variables = dict(args.variables) if args.variables else {}
    content = render_template(args.template, variables)

    # Build and run pipeline
    config = yaml.safe_load(content)
    pipeline = base.Pipeline.build(config)
    pipeline.run_all()


if __name__ == "__main__":
    main()
