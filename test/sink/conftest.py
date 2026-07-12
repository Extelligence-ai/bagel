"""Shared fixtures for sink tests: a fake paho client and an isolated MQTT sink factory."""

import pathlib
from collections.abc import Callable, Iterator
from types import SimpleNamespace

import pytest

pytest.importorskip("paho")

from settings import settings
from src.sink import base as sink_base
from src.sink import mqtt


class FakePahoClient:
    """A stand-in for paho.Client that delivers configured retained messages."""

    def __init__(self, callback_api_version: object = None, client_id: str | None = None) -> None:
        self.on_message = None
        self.retained: dict[str, list[bytes]] = {}
        self.subscriptions: list[str] = []
        self.topic_callbacks: dict[str, object] = {}
        self._connected = False

    # -- connection ------------------------------------------------------------
    def username_pw_set(self, username: str, password: str | None = None) -> None:
        pass

    def is_connected(self) -> bool:
        return self._connected

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        self._connected = True

    def loop_start(self) -> None:
        pass

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        self._connected = False

    # -- pub/sub ----------------------------------------------------------------
    def subscribe(self, topic: str) -> None:
        self.subscriptions.append(topic)
        # Deliver retained messages synchronously, like a broker on subscribe.
        for retained_topic, payloads in self.retained.items():
            if topic in (mqtt.DISCOVERY_WILDCARD, retained_topic):
                for payload in payloads:
                    self.deliver(retained_topic, payload)

    def unsubscribe(self, topic: str) -> None:
        self.subscriptions = [s for s in self.subscriptions if s != topic]

    def message_callback_add(self, topic: str, callback: object) -> None:
        self.topic_callbacks[topic] = callback

    def message_callback_remove(self, topic: str) -> None:
        self.topic_callbacks.pop(topic, None)

    def deliver(self, topic: str, payload: bytes) -> None:
        """Simulate a message arriving from the broker."""
        message = SimpleNamespace(topic=topic, payload=payload)
        if topic in self.topic_callbacks:
            self.topic_callbacks[topic](self, None, message)
        elif self.on_message is not None:
            self.on_message(self, None, message)


_PORT_COUNTER = iter(range(20000, 30000))

MakeSink = Callable[..., "mqtt.TopicSink"]


@pytest.fixture
def make_sink(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[MakeSink]:
    """Build an MQTT sink wired to a FakePahoClient, isolated per test."""
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path / "cache"))
    sink_base._global_sink_singletons.clear()

    def _make(retained: dict[str, list[bytes]] | None = None, **kwargs: object) -> mqtt.TopicSink:
        fake = FakePahoClient()
        fake.retained = retained or {}
        monkeypatch.setattr(mqtt.paho, "Client", lambda **_: fake)
        sink = mqtt.TopicSink(
            host="broker.test", port=next(_PORT_COUNTER), discovery_seconds=0.0, **kwargs
        )
        sink._fake = fake  # expose for assertions/injection
        return sink

    yield _make
    sink_base._global_sink_singletons.clear()
