"""Exercise browser enrollment boundaries and the real durable client lifecycle."""

import time
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from settings import settings
from src import mcp_compat
from src.sink.publish import identity, pairing, pairing_ui
from test.sink.publish.test_pairing import CODE, response


def wait_for(predicate: object) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Worker did not finish")


@pytest.fixture
def ui(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple:
    monkeypatch.setattr(settings, "FLEET_PAIRING_UI_ENABLED", True)
    monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path))
    monkeypatch.setattr(settings, "FLEET_ENROLL_URL", "https://fleet.example")
    server = mcp_compat.create_server("test", "127.0.0.1", 8000)
    manager = pairing_ui.register(server)
    app = server.sse_app()
    return TestClient(app, base_url="http://localhost:8000"), manager


def headers() -> dict:
    return {"Origin": "http://localhost:8000", "X-Bagel-CSRF": pairing_ui.CSRF}


def test_browser_cannot_enroll_cross_origin_or_use_arbitrary_service(ui: tuple) -> None:
    client, manager = ui
    body = {"code": CODE}
    page = client.get("/fleet/connect")
    assert page.status_code == 200
    assert pairing_ui.CSRF in page.text
    assert page.headers["referrer-policy"] == "no-referrer"
    assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
    assert client.get("/fleet/connect", headers={"Host": "attacker.example"}).status_code == 403
    assert client.post("/fleet/connect/start", json=body).status_code == 403
    assert (
        client.post(
            "/fleet/connect/start",
            json=body,
            headers={**headers(), "Origin": "https://attacker.example"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/fleet/connect/start", json=body, headers={**headers(), "X-Bagel-CSRF": "wrong"}
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/fleet/connect/start",
            json={**body, "url": "https://attacker.example"},
            headers=headers(),
        ).status_code
        == 409
    )
    assert not (manager.directory / "pairing-pending.json").exists()
    assert (
        client.post(
            "/fleet/connect/start",
            content="x" * 5000,
            headers={**headers(), "Content-Type": "application/json"},
        ).status_code
        == 413
    )


def test_browser_uses_real_key_then_installs_and_reloads_without_terminal(
    ui: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, manager = ui
    seen = []

    def exchange(pending: dict) -> dict:
        seen.append(pending)
        return {"status": "issued", **response(pending)}

    async def activate() -> bool:
        return True

    monkeypatch.setattr(pairing, "exchange", exchange)
    monkeypatch.setattr(pairing, "activate", activate)
    result = client.post(
        "/fleet/connect/start",
        json={"code": CODE},
        headers=headers(),
    )
    assert result.status_code == 200
    wait_for(lambda: manager.status()["status"] == "connected")
    public = client.get("/fleet/connect/status")
    assert CODE not in public.text
    assert "PRIVATE KEY" not in public.text
    assert public.json()["streams_reloaded"] is True
    assert identity.is_enrolled(manager.directory)
    assert len(seen) == 1
    assert (
        client.post(
            "/fleet/connect/start",
            json={"code": CODE},
            headers=headers(),
        ).status_code
        == 409
    )


def test_disabled_ui_registers_no_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "FLEET_PAIRING_UI_ENABLED", False)
    server = mcp_compat.create_server("test", "127.0.0.1", 8000)
    assert pairing_ui.register(server) is None
    assert TestClient(server.sse_app()).get("/fleet/connect").status_code == 404


def test_restart_resumes_pending_key_without_a_browser_click(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending = pairing.prepare(tmp_path, "https://fleet.example", CODE)
    monkeypatch.setattr(settings, "FLEET_PAIRING_UI_ENABLED", True)
    monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path))
    monkeypatch.setattr(settings, "FLEET_ENROLL_URL", "https://fleet.example")
    seen = []

    def exchange(request: dict) -> dict:
        seen.append(request)
        raise ValueError("denied with a SECRET in its body")

    monkeypatch.setattr(pairing, "exchange", exchange)
    manager = pairing_ui.register(mcp_compat.create_server("test", "127.0.0.1", 8000))
    wait_for(lambda: manager.status()["status"] == "error")
    assert seen == [pending]
    assert "SECRET" not in str(manager.status())
    assert pairing.prepare(tmp_path, "https://fleet.example", CODE) == pending
