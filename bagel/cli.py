"""The ``bagel`` command-line entry point.

Thin dispatcher over the two existing entry points so that
``bagel serve`` boots the MCP server and ``bagel run <template>`` runs a
pipeline. Each subcommand delegates to the module that already owns its
argument parsing, so behaviour matches ``python -m bagel.server`` /
``python -m bagel.run`` exactly.
"""

import argparse
import sys


def main(argv: list[str] | None = None) -> None:
    """Dispatch a ``bagel`` subcommand."""
    argv = sys.argv[1:] if argv is None else argv

    parser = argparse.ArgumentParser(
        prog="bagel", description="Bagel: chat with your physical data."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run the Bagel MCP server.")
    serve.add_argument("--transport", help='MCP transport ("sse" or "streamable-http").')
    serve.add_argument("--host", help="Host to bind the server to.")
    serve.add_argument("--port", type=int, help="Port to bind the server to.")

    sub.add_parser(
        "run",
        help="Run a pipeline from a Jinja/YAML template (see `bagel run --help`).",
        add_help=False,  # forwarded verbatim to bagel.run's own parser
    )

    # Split argv at the subcommand so `run`'s own parser sees the rest untouched.
    if argv and argv[0] == "run":
        from bagel import run

        sys.argv = ["bagel run", *argv[1:]]
        run.main()
        return

    args = parser.parse_args(argv)
    if args.command == "serve":
        from bagel import server

        server.main(transport=args.transport, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
