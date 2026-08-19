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


def test_combined_app_threads_host_into_factories_that_accept_it() -> None:
    # On MCP SDK v2 the app factories take the host and use it for
    # DNS-rebinding validation; a combined app built without it rejects
    # non-localhost requests with 421. Factories without the parameter
    # (SDK v1) must still be called plainly.
    received = {}

    class _V2Style:
        def streamable_http_app(self, host: str | None = None):  # noqa: ANN202
            received["http"] = host
            return _stub_app()

        def sse_app(self, host: str | None = None):  # noqa: ANN202
            received["sse"] = host
            return _stub_app()

    class _V1Style:
        def streamable_http_app(self):  # noqa: ANN202
            return _stub_app()

        def sse_app(self):  # noqa: ANN202
            return _stub_app()

    def _stub_app():  # noqa: ANN202
        class _Router:
            def __init__(self) -> None:
                self.routes: list = []

        class _App:
            def __init__(self) -> None:
                self.router = _Router()
                self.routes: list = []

        return _App()

    mcp_compat.combined_app(_V2Style(), host="0.0.0.0")  # noqa: S104
    assert received == {"http": "0.0.0.0", "sse": "0.0.0.0"}  # noqa: S104
    mcp_compat.combined_app(_V1Style(), host="0.0.0.0")  # noqa: S104 -- must not raise
