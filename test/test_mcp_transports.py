"""Both MCP transports must be reachable on one port (#168).

Codex's native MCP client speaks only streamable HTTP (/mcp); every setup
runbook and existing user config speaks SSE (/sse). The default server must
serve both so neither side needs transport configuration.
"""

import threading
import time

import httpx
import uvicorn
from starlette.testclient import TestClient

from src import mcp_compat

INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "transport-guard", "version": "0"},
    },
}


def _client() -> TestClient:
    server = mcp_compat.create_server(name="guard", host="127.0.0.1", port=0)
    return TestClient(mcp_compat.combined_app(server))


def test_streamable_http_initialize_succeeds_on_mcp() -> None:
    with _client() as client:
        response = client.post(
            "/mcp",
            json=INITIALIZE,
            headers={"Accept": "application/json, text/event-stream"},
        )
        assert response.status_code == 200, response.text


def test_legacy_sse_endpoint_still_streams() -> None:
    # A real server on an ephemeral port: the in-process TestClient deadlocks
    # on the endless SSE stream, so read only the response headers over a
    # socket with a hard timeout.
    server = mcp_compat.create_server(name="guard-sse", host="127.0.0.1", port=0)
    app = mcp_compat.combined_app(server)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
    uv_server = uvicorn.Server(config)
    thread = threading.Thread(target=uv_server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not uv_server.started:
        assert time.time() < deadline, "server did not start"
        time.sleep(0.05)
    (socket_info,) = uv_server.servers[0].sockets
    port = socket_info.getsockname()[1]
    try:
        with httpx.stream("GET", f"http://127.0.0.1:{port}/sse", timeout=5) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
    finally:
        uv_server.should_exit = True
        thread.join(timeout=10)
