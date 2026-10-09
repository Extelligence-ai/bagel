"""`bagel-mcp` flags: an MCP client that spawns the server needs `--transport stdio`."""

import pytest

from bagel_mcp import mcp_compat, server
from bagel_mcp.settings import settings


@pytest.fixture
def run_server(monkeypatch: pytest.MonkeyPatch) -> dict:
    received: dict = {}

    def fake(instance: object, transport: str, host: str, port: int) -> None:
        received.update(instance=instance, transport=transport, host=host, port=port)

    monkeypatch.setattr(mcp_compat, "run_server", fake)
    return received


def test_defaults_come_from_settings(run_server: dict) -> None:
    server.main([])
    assert run_server == {
        "instance": server.server,
        "transport": settings.MCP_TRANSPORT,
        "host": settings.MCP_SERVER_HOST,
        "port": settings.MCP_SERVER_PORT,
    }


def test_flags_override_settings(run_server: dict) -> None:
    server.main(["--transport", "stdio", "--host", "0.0.0.0", "--port", "8100"])  # noqa: S104
    assert run_server["transport"] == "stdio"
    assert run_server["host"] == "0.0.0.0"  # noqa: S104
    assert run_server["port"] == 8100


def test_unknown_transport_is_rejected(run_server: dict) -> None:
    with pytest.raises(SystemExit):
        server.main(["--transport", "carrier-pigeon"])
    assert run_server == {}
