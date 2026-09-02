"""The fleet gate: one function every fleet entry point calls first.

It must never import paho at module import time (the rest of Bagel never pays
for MQTT), must honor the FLEET_ENABLED kill switch, and must turn a missing
optional dependency into a typed error with install guidance.
"""

import builtins
import importlib
import sys

import pytest

from settings import settings
from src.sink import publish


def test_disabled_wins_over_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "FLEET_ENABLED", False)
    with pytest.raises(publish.FleetDisabledError, match="FLEET_ENABLED"):
        publish.require_fleet()


def test_missing_paho_raises_typed_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "FLEET_ENABLED", True)
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "paho" or name.startswith("paho."):
            raise ImportError("No module named 'paho'")
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "paho", raising=False)
    monkeypatch.delitem(sys.modules, "paho.mqtt", raising=False)
    monkeypatch.delitem(sys.modules, "paho.mqtt.client", raising=False)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(publish.FleetNotInstalledError, match="fleet"):
        publish.require_fleet()


def test_gate_passes_when_installed_and_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("paho")
    monkeypatch.setattr(settings, "FLEET_ENABLED", True)
    assert publish.require_fleet() is None


def test_publish_package_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing src.sink.publish must not pull paho or cryptography in.

    Only require_fleet() imports paho; only identity.py's functions import
    cryptography (see identity.py's module docstring) -- so importing the
    package itself must stay light regardless of which submodule a caller
    reaches for next.
    """
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish", raising=False)
    importlib.import_module("src.sink.publish")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )


def test_setting_default_is_enabled() -> None:
    assert settings.model_fields["FLEET_ENABLED"].default is True
