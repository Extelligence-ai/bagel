"""Tests for the MQTT topic sink, using a fake paho client (no broker needed)."""

import json
import pathlib
from collections.abc import Callable, Iterator
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

pytest.importorskip("paho")

from settings import settings
from src.pipeline.base import Cadence, OnEvent
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


# -- pure helpers ------------------------------------------------------------------


def test_normalize_payload_json_object() -> None:
    assert mqtt.normalize_payload(b'{"temp": -18.5, "door": "closed"}') == {
        "temp": -18.5,
        "door": "closed",
    }


def test_normalize_payload_bare_scalar_and_array() -> None:
    assert mqtt.normalize_payload(b"23.5") == {"value": 23.5}
    assert mqtt.normalize_payload(b"[1, 2]") == {"value": [1, 2]}


def test_normalize_payload_non_json_text() -> None:
    assert mqtt.normalize_payload(b"ON") == {"payload": "ON"}


def test_normalize_payload_invalid_utf8() -> None:
    result = mqtt.normalize_payload(b"\xff\xfe")
    assert "payload" in result


def test_infer_struct_unifies_samples() -> None:
    struct = mqtt.infer_struct([{"temp": -18.5}, {"temp": -17.0, "door": "open"}])
    assert struct.field("temp").type == pa.float64()
    assert struct.field("door").type == pa.string()


def test_infer_struct_nested() -> None:
    struct = mqtt.infer_struct([{"gps": {"lat": 1.0, "lon": 2.0}}])
    assert pa.types.is_struct(struct.field("gps").type)


def test_infer_struct_empty_raises() -> None:
    with pytest.raises(ValueError, match="zero samples"):
        mqtt.infer_struct([])


# -- sink behavior ------------------------------------------------------------------


def test_discovery_finds_retained_topics(make_sink: MakeSink) -> None:
    sink = make_sink(
        retained={
            "freezer/1/status": [b'{"temp": -18.5}'],
            "freezer/2/status": [b'{"temp": -17.9}'],
        }
    )
    assert sink.available_topics == ["freezer/1/status", "freezer/2/status"]
    # Discovery wildcard is released after the window.
    assert mqtt.DISCOVERY_WILDCARD not in sink._fake.subscriptions


def test_struct_and_definition_from_samples(make_sink: MakeSink) -> None:
    sink = make_sink(retained={"freezer/1/status": [b'{"temp": -18.5, "door": "closed"}']})
    struct = sink._struct("freezer/1/status")
    assert struct.field("temp").type == pa.float64()
    definition = sink._definition("freezer/1/status")
    assert json.loads(definition) == {"temp": -18.5, "door": "closed"}


def test_unseen_topic_falls_back_after_timeout(make_sink: MakeSink) -> None:
    sink = make_sink(retained={}, schema_timeout_seconds=0.1)
    struct = sink._struct("never/seen")
    assert struct.field("payload").type == pa.string()


def test_subscribe_buffers_messages(make_sink: MakeSink) -> None:
    sink = make_sink(retained={"plant/pump": [b'{"pressure": 4.2}']})
    sink.subscribe("plant/pump")

    sink._fake.deliver("plant/pump", b'{"pressure": 3.9}')
    sink._fake.deliver("plant/pump", b'{"pressure": 1.1}')

    buffers = list(pathlib.Path(settings.CACHE_DIRECTORY).rglob("current.jsonl"))
    assert len(buffers) == 1
    lines = [json.loads(line) for line in buffers[0].read_text().splitlines()]
    # The retained sample is delivered again on topic subscribe, then the two live ones.
    assert [record["plant/pump"]["pressure"] for record in lines] == [4.2, 3.9, 1.1]
    assert all(settings.TIMESTAMP_SECONDS_COLUMN_NAME in record for record in lines)


def test_on_event_pipeline_fires_on_live_messages(make_sink: MakeSink) -> None:
    # Retained message includes "t" since brokers re-deliver retained on subscribe.
    sink = make_sink(retained={"freezer/1/status": [b'{"temp": -18.5, "t": 0.0}']})

    pipeline = MagicMock()
    pipeline.cadence = Cadence(
        topic="freezer/1/status",
        when=OnEvent(predicate="\"freezer/1/status\"['temp'] > -15"),
    )
    sink.subscribe(
        "freezer/1/status",
        pipeline=pipeline,
        extract_timestamp=lambda message: message["t"],
    )

    for t, temp in ((1.0, -18.0), (2.0, -12.0), (3.0, -11.5), (4.0, -18.0)):
        sink._fake.deliver(
            "freezer/1/status", json.dumps({"temp": temp, "t": t}).encode()
        )

    # One rising edge at t=2.0 (sustained warm reading counts once).
    assert [call.args[0] for call in pipeline.run_at.call_args_list] == [2.0]


def test_close_unsubscribes_and_disconnects(make_sink: MakeSink) -> None:
    sink = make_sink(retained={"a/b": [b'{"x": 1}']})
    sink.subscribe("a/b")
    fake = sink._fake
    sink.close()
    assert "a/b" not in fake.topic_callbacks
    assert not fake.is_connected()


def test_guess_defaults() -> None:
    from src.di.types import topic_sink

    assert topic_sink.guess_port(topic_sink.TopicSink.MQTT) == 1883
    assert topic_sink.guess_host(topic_sink.TopicSink.MQTT) in (
        "localhost",
        "host.docker.internal",
    )
