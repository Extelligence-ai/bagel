"""Export the actual MCP tools/list schema from a configured Bagel environment."""

import asyncio
import json
import sys
from pathlib import Path


def main() -> None:
    """Print JSON to stdout; importing the server does not start subscriptions."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from server import server

    tools = asyncio.run(server.list_tools())
    print(json.dumps([tool.model_dump(mode="json") for tool in tools], indent=2))  # noqa: T201


if __name__ == "__main__":
    main()
