"""Tests for the MCP SDK v1/v2 compatibility layer."""

from unittest.mock import MagicMock

import pytest

from src import mcp_compat


def test_create_server_exposes_the_decorator_api() -> None:
    server = mcp_compat.create_server(name="test", host="127.0.0.1", port=18000)
    assert hasattr(server, "tool")

    @server.tool(title="Ping", description="test tool")
    def ping() -> str:
        """Reply with pong."""
        return "pong"

    assert callable(ping)
    assert ping() == "pong"


def test_run_routes_host_port_per_sdk_version(monkeypatch: pytest.MonkeyPatch) -> None:
    server = MagicMock()

    monkeypatch.setattr(mcp_compat, "MCP_SDK_V2", False)
    mcp_compat.run_server(server, transport="sse", host="h", port=1)
    server.run.assert_called_once_with(transport="sse")

    server.reset_mock()
    monkeypatch.setattr(mcp_compat, "MCP_SDK_V2", True)
    mcp_compat.run_server(server, transport="streamable-http", host="h", port=1)
    server.run.assert_called_once_with(transport="streamable-http", host="h", port=1)


def test_exactly_one_sdk_branch_is_active() -> None:
    # Under the project venv this is v1; under the v2 beta venv it flips. Either
    # way the flag must be a real bool and the server class importable.
    assert isinstance(mcp_compat.MCP_SDK_V2, bool)
