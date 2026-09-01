"""Publisher contract: wire topics, retain rules, and the lazy-import invariant."""

import importlib
import sys
from typing import Any

import pytest

from src.sink.publish import publisher


class TestWireTopic:
    def test_builds_namespaced_topic(self) -> None:
        assert publisher.wire_topic("acme", "r7", "channels") == "bagel/v1/acme/r7/channels"

    @pytest.mark.parametrize("kind", ["schema", "channels", "events", "heartbeat", "cmd"])
    def test_all_contract_kinds_accepted(self, kind: str) -> None:
        assert publisher.wire_topic("t", "r", kind).endswith("/" + kind)

    def test_unknown_kind_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown kind"):
            publisher.wire_topic("t", "r", "video")


class RecordingPublisher(publisher.Publisher):
    """Minimal concrete Publisher recording publish calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], bool]] = []
        self._connected = False

    def connect(self) -> None:
        self._connected = True

    def publish(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        retain: bool = False,
        timeout_s: float = 10.0,
    ) -> None:
        self.calls.append((kind, payload, retain))

    def close(self) -> None:
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected


class TestRetainRules:
    @pytest.mark.parametrize(
        ("method", "kind", "retain"),
        [
            ("publish_schema", "schema", True),
            ("publish_heartbeat", "heartbeat", True),
            ("publish_channels", "channels", False),
            ("publish_event", "events", False),
        ],
    )
    def test_helper_sets_kind_and_retain(self, method: str, kind: str, retain: bool) -> None:
        p = RecordingPublisher()
        getattr(p, method)({"v": 1})
        assert p.calls == [(kind, {"v": 1}, retain)]


def test_publisher_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.publisher", raising=False)
    importlib.import_module("src.sink.publish.publisher")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)
