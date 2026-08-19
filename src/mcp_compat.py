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

import inspect
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


def combined_app(server: Any, host: str | None = None) -> Any:  # noqa: ANN401
    """One ASGI app serving both transports: /sse (+ /messages/) and /mcp.

    Codex's native MCP client speaks only streamable HTTP, while the setup
    runbooks and existing user configs speak SSE. Route sets are disjoint, so
    the streamable app (whose lifespan owns the session manager) absorbs the
    SSE routes.

    On SDK v2 the app factories take the host and use it for DNS-rebinding
    validation; without it a non-loopback bind rejects LAN requests with 421.
    v1 factories take no arguments (host arrived at construction).
    """

    def _build(factory: Any) -> Any:  # noqa: ANN401
        if host is not None and "host" in inspect.signature(factory).parameters:
            return factory(host=host)
        return factory()

    http_app = _build(server.streamable_http_app)
    http_app.router.routes.extend(_build(server.sse_app).routes)
    return http_app


def run_server(server: Any, transport: str, host: str, port: int) -> None:  # noqa: ANN401
    """Run the server: "both" (default), "sse", or "streamable-http".

    "both" composes the two transport apps onto one uvicorn (#168). A single
    named transport falls through to the SDK's own runner: v2 accepts
    host/port as transport kwargs; v1 already received them at construction.
    """
    if transport == "both" and hasattr(server, "streamable_http_app"):
        import uvicorn

        uvicorn.run(combined_app(server, host=host), host=host, port=port)
        return
    if transport == "both":  # SDK without app composition: closest single transport
        transport = "streamable-http"
    if MCP_SDK_V2:
        server.run(transport=transport, host=host, port=port)
    else:
        server.run(transport=transport)
