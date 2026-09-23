"""Delivery participates in the server's fleet lifecycle and feature gate."""

import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from settings import settings
from src.sink.publish import FleetDisabledError, control
from src.sink.publish.delivery import main, service


@pytest.fixture(autouse=True)
def isolated_process(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(service, "_process", None)


def test_default_and_kill_switch_do_not_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    spawn = MagicMock()
    monkeypatch.setattr(service.subprocess, "Popen", spawn)
    monkeypatch.setattr(settings, "FLEET_DELIVERY_SOURCES", None)
    assert service.start() is False
    monkeypatch.setattr(settings, "FLEET_DELIVERY_SOURCES", "/a/config.json")
    monkeypatch.setattr(settings, "FLEET_ENABLED", False)
    assert service.start() is False
    spawn.assert_not_called()


def test_cli_obeys_fleet_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "FLEET_ENABLED", False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "delivery",
            "--identity",
            "/x/identity.yaml",
            "--sources",
            "/x/sources.json",
            "--state",
            "/x/state",
        ],
    )
    with pytest.raises(FleetDisabledError):
        main.main()


def test_start_once_and_stop_process_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "sources.json"
    source.write_text('{"sources": {}}')
    monkeypatch.setattr(settings, "FLEET_ENABLED", True)
    monkeypatch.setattr(settings, "FLEET_DELIVERY_SOURCES", str(source))
    monkeypatch.setattr(settings, "FLEET_CONTROL_URL", "https://fleet.example")
    process = MagicMock(pid=123)
    process.poll.return_value = None
    spawn = MagicMock(return_value=process)
    kill = MagicMock()
    monkeypatch.setattr(service.subprocess, "Popen", spawn)
    monkeypatch.setattr(service.os, "killpg", kill)
    assert service.start() and service.start()
    spawn.assert_called_once()
    assert "src.sink.publish.delivery.main" in spawn.call_args.args[0]
    process.wait.side_effect = [subprocess.TimeoutExpired("delivery", 15), None]
    service.stop()
    assert kill.call_count == 2
    assert service._process is None


def test_unenroll_stops_delivery_before_removing_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(service, "stop", lambda: calls.append("stop"))
    monkeypatch.setattr(control.startup, "fleet_service", lambda: None)
    monkeypatch.setattr(control.identity, "delete_identity", lambda _: calls.append("delete") or [])
    monkeypatch.setattr(control, "_persist_streams", lambda _: False)
    control.unenroll_identity()
    assert calls == ["stop", "delete"]


def test_channel_reloads_rotated_certificate_pointers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import yaml

    identity_file = tmp_path / "identity.yaml"
    config = {
        "renew_url": "https://fleet.example",
        "cert_file": "first.crt",
        "key_file": "first.key",
    }
    identity_file.write_text(yaml.safe_dump(config))
    context = MagicMock()
    monkeypatch.setattr(main.ssl, "create_default_context", lambda **_: context)
    channel = main.Channel(identity_file)
    channel.config()
    config.update(cert_file="renewed.crt", key_file="renewed.key")
    identity_file.write_text(yaml.safe_dump(config))
    channel.config()
    assert context.load_cert_chain.call_args.args == (
        str(tmp_path / "renewed.crt"),
        str(tmp_path / "renewed.key"),
    )
    config["cert_file"] = "../escape.crt"
    identity_file.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match="inside the identity directory"):
        channel.config()
    config.update(cert_file="renewed.crt", renew_url="http://fleet.example")
    identity_file.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match="HTTPS"):
        channel.config()
