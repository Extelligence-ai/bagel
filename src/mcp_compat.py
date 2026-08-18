"""Compatibility layer across MCP Python SDK v1 and v2.

The MCP spec revision of 2026-07-28 ships as `mcp` v2, which renames `FastMCP` to
`MCPServer` (the legacy `mcp.server.fastmcp` module is removed) and moves the
host/port configuration from the constructor to `run()`. The tool decorator API --
including the `title`/`description` keywords Bagel uses -- is unchanged, and both
the "sse" and "streamable-http" transports remain available.

This module isolates those differences so `server.py` runs unmodified on either
major version. Verified against `mcp==2.0.0b1` and the pinned 1.x release.
See https://blog.modelcontextprotocol.io/posts/sdk-betas-2026-07-28/.
"""

from typing import Any

try:  # mcp >= 2.0
    from mcp.server.mcpserver import MCPServer as _Server

    MCP_SDK_V2 = True
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server

    MCP_SDK_V2 = False


def create_server(name: str, host: str, port: int, instructions: str | None = None) -> Any:  # noqa: ANN401 -- SDK type varies by major version
    """Create the MCP server object for whichever SDK major version is installed.

    v1 takes host/port at construction; v2 takes them at `run()` (see `run_server`).
    `instructions` is delivered to every client at the initialize handshake on
    both majors: it is the one server-authored prompt surface agents read.
    """
    if MCP_SDK_V2:
        return _Server(name=name, instructions=instructions)
    return _Server(name=name, instructions=instructions, host=host, port=port)


def run_server(server: Any, transport: str, host: str, port: int) -> None:  # noqa: ANN401
    """Run the server on the given transport ("sse" or "streamable-http").

    v2 accepts host/port as transport kwargs; v1 already received them at
    construction and accepts only the transport name.
    """
    if MCP_SDK_V2:
        server.run(transport=transport, host=host, port=port)
    else:
        server.run(transport=transport)
