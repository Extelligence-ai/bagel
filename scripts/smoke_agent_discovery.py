"""Exercise actual read-only MCP calls over stdio against the bundled CSV fixture."""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def requests() -> list[dict]:
    """Inspect first, then compute and preview using the fixture's declared time axis."""
    common = {
        "path": "./data/sample/pyarrow/csv/flight.csv",
        "args": {"timestamp_column": "t", "timestamp_format": "seconds"},
    }
    return [
        {"tool": "describe_data_source", "arguments": common},
        {"tool": "describe_topic", "arguments": {**common, "topic": "message"}},
        {
            "tool": "query_messages",
            "arguments": {
                **common,
                "topic": "message",
                "sql_statement": (
                    "SELECT COUNT(*) AS samples, MIN(\"message\"['accel_x']) AS min_accel_x "
                    'FROM "message"'
                ),
            },
        },
        {
            "tool": "preview_pipeline",
            "arguments": {
                **common,
                "event_topic": "message",
                "predicate": "\"message\"['accel_x'] < -10",
                "pre_seconds": 1.0,
                "post_seconds": 1.0,
            },
        },
    ]


async def run(output: Path) -> None:
    """Retain initialize, tools/list, and complete request/response pairs."""
    env = {
        **os.environ,
        "CACHE_DIRECTORY": str(output / "cache"),
        "ARTIFACT_DIRECTORY": str(output / "artifacts"),
        "STARTUP_PIPELINES_FILE": "",
        "USER_CAPABILITIES_DIRECTORY": str(output / "capabilities"),
    }
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-c", "import server; server.server.run(transport='stdio')"],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
    )
    with (output / "server-stderr.txt").open("w") as stderr:
        async with stdio_client(parameters, errlog=stderr) as (read, write):
            async with ClientSession(read, write) as session:
                initialized = await session.initialize()
                catalog = await session.list_tools()
                (output / "initialize.json").write_text(initialized.model_dump_json(indent=2))
                (output / "tools.json").write_text(catalog.model_dump_json(indent=2))
                for request in requests():
                    result = await session.call_tool(request["tool"], request["arguments"])
                    record = {"request": request, "response": result.model_dump(mode="json")}
                    (output / f"{request['tool']}.json").write_text(
                        json.dumps(record, indent=2) + "\n"
                    )
                    if result.isError:
                        raise RuntimeError(f"MCP smoke failed: {request['tool']}; see {output}")
    preview = json.loads((output / "preview_pipeline.json").read_text())
    data = preview["response"]["structuredContent"]
    expected = {"total_seconds": 59.5, "event_count": 2, "kept_seconds": 4.0}
    if any(data[key] != value for key, value in expected.items()):
        raise RuntimeError(f"Fixture time-axis/event validation failed; inspect {output}")
    print(f"MCP initialize, tools/list, and {len(requests())} read-only calls passed: {output}")  # noqa: T201


def main() -> None:
    """Run in the installed Bagel Python environment without an LLM account."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    asyncio.run(run(output))


if __name__ == "__main__":
    main()
