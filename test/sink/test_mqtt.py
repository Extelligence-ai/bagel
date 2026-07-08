"""Tests for the MQTT topic sink, using a fake paho client (no broker needed)."""

import json
import pathlib
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

pytest.importorskip("paho")

from conftest import _PORT_COUNTER, FakePahoClient, MakeSink

from settings import settings
from src.pipeline.base import Cadence, OnEvent
from src.sink import mqtt

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


# -- extras: TLS / transport / timestamp field ---------------------------------------


def test_websockets_transport_and_tls_configure_paho(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path / "cache"))
    captured: dict[str, object] = {}

    class RecordingFake(FakePahoClient):
        def __init__(self, callback_api_version: object = None, client_id: str | None = None,
                     transport: str = "tcp") -> None:
            super().__init__(callback_api_version, client_id)
            captured["transport"] = transport
            self.tls_args: dict[str, object] | None = None

        def tls_set(self, ca_certs: str | None = None) -> None:
            captured["ca_certs"] = ca_certs

        def tls_insecure_set(self, value: bool) -> None:
            captured["tls_insecure"] = value

    monkeypatch.setattr(mqtt.paho, "Client", RecordingFake)
    mqtt.TopicSink(
        host="broker.test",
        port=next(_PORT_COUNTER),
        discovery_seconds=0.0,
        transport="websockets",
        tls=True,
        tls_ca_certs="/etc/ssl/ca.pem",
        tls_insecure=True,
    )
    assert captured == {
        "transport": "websockets",
        "ca_certs": "/etc/ssl/ca.pem",
        "tls_insecure": True,
    }


def test_invalid_transport_and_unit_rejected() -> None:
    with pytest.raises(ValueError, match="transport"):
        mqtt.TopicSink(host="h", port=next(_PORT_COUNTER), transport="carrier-pigeon")
    with pytest.raises(ValueError, match="timestamp_unit"):
        mqtt.TopicSink(host="h", port=next(_PORT_COUNTER), timestamp_unit="fortnight")


def test_timestamp_field_used_for_buffered_messages(make_sink: MakeSink) -> None:
    sink = make_sink(
        retained={"plant/pump": [b'{"pressure": 4.2, "ts": 1700000000500}']},
        timestamp_field="ts",
        timestamp_unit="millisecond",
    )
    sink.subscribe("plant/pump")
    sink._fake.deliver("plant/pump", b'{"pressure": 3.9, "ts": 1700000001000}')
    sink._fake.deliver("plant/pump", b'{"pressure": 3.7}')  # missing field -> arrival time

    buffers = list(pathlib.Path(settings.CACHE_DIRECTORY).rglob("current.jsonl"))
    lines = [json.loads(line) for line in buffers[0].read_text().splitlines()]
    stamps = [record[settings.TIMESTAMP_SECONDS_COLUMN_NAME] for record in lines]
    assert stamps[0] == pytest.approx(1700000000.5)  # retained, redelivered on subscribe
    assert stamps[1] == pytest.approx(1700000001.0)
    assert stamps[2] > 1750000000  # fell back to (much later) arrival time
