"""Tests for Sparkplug B payload decoding and its wiring into the MQTT sink."""

import json
import pathlib

import pytest

pytest.importorskip("paho")

from settings import settings
from src.sink.sparkplug import decode
from src.sink.sparkplug import sparkplug_b_minimal_pb2 as pb

# The fake-broker `make_sink` fixture comes from test/sink/conftest.py.

SP_TOPIC = "spBv1.0/plant1/DDATA/edge1/press42"


def _payload(**metrics: object) -> bytes:
    message = pb.Payload(timestamp=1_700_000_000_000, seq=7)
    for name, value in metrics.items():
        metric = message.metrics.add()
        metric.name = name
        if isinstance(value, bool):
            metric.boolean_value = value
        elif isinstance(value, float):
            metric.double_value = value
        elif isinstance(value, int):
            metric.long_value = value
        elif isinstance(value, str):
            metric.string_value = value
        elif value is None:
            metric.is_null = True
    return message.SerializeToString()


def test_topic_namespace_detection() -> None:
    assert decode.is_sparkplug_topic(SP_TOPIC)
    assert not decode.is_sparkplug_topic("freezer/1/status")


def test_decode_scalar_metrics() -> None:
    decoded = decode.decode_payload(
        _payload(temperature=87.5, cycles=12000, running=True, mode="auto")
    )
    assert decoded == {
        "timestamp": 1_700_000_000_000,
        "seq": 7,
        "temperature": 87.5,
        "cycles": 12000,
        "running": True,
        "mode": "auto",
    }


def test_decode_null_metric_and_alias_fallback() -> None:
    message = pb.Payload(timestamp=1, seq=2)
    null_metric = message.metrics.add()
    null_metric.name = "offline_sensor"
    null_metric.is_null = True
    aliased = message.metrics.add()
    aliased.alias = 42
    aliased.double_value = 3.14

    decoded = decode.decode_payload(message.SerializeToString())
    assert decoded["offline_sensor"] is None
    assert decoded["alias_42"] == 3.14


def test_decode_tolerates_unknown_fields() -> None:
    # Append an unknown length-delimited field (e.g. a DataSet at field 17 of a
    # Metric would be skipped the same way): field 99, wire type 0 (varint).
    raw = _payload(temp=1.0) + bytes([0b10111000, 0b00000110, 0x2A])  # field 99 varint 42
    decoded = decode.decode_payload(raw)
    assert decoded["temp"] == 1.0


def test_sink_decodes_sparkplug_topics_end_to_end(make_sink) -> None:  # noqa: ANN001
    sink = make_sink(
        retained={SP_TOPIC: [_payload(temperature=85.0, running=True)]},
        timestamp_field="timestamp",
        timestamp_unit="millisecond",
    )
    assert sink.available_topics == [SP_TOPIC]
    assert sink._type_name(SP_TOPIC) == "mqtt/sparkplugb"

    struct = sink._struct(SP_TOPIC)
    assert struct.field("temperature").type == "double"
    assert struct.field("running").type == "bool"

    sink.subscribe(SP_TOPIC)
    sink._fake.deliver(SP_TOPIC, _payload(temperature=91.25, running=True))

    buffers = list(pathlib.Path(settings.CACHE_DIRECTORY).rglob("current.jsonl"))
    lines = [json.loads(line) for line in buffers[0].read_text().splitlines()]
    assert lines[-1][SP_TOPIC]["temperature"] == 91.25
    # Sparkplug's own epoch-ms timestamp drives the buffer timestamps.
    assert lines[-1][settings.TIMESTAMP_SECONDS_COLUMN_NAME] == pytest.approx(1_700_000_000.0)


def test_sparkplug_disabled_falls_back_to_raw(make_sink) -> None:  # noqa: ANN001
    sink = make_sink(
        retained={SP_TOPIC: [_payload(temperature=85.0)]}, sparkplug_b=False
    )
    assert sink._type_name(SP_TOPIC) == "mqtt/json"
    struct = sink._struct(SP_TOPIC)
    assert "payload" in struct.names  # binary protobuf treated as opaque text


def test_invalid_protobuf_on_sparkplug_topic_falls_back(make_sink) -> None:  # noqa: ANN001
    sink = make_sink(retained={SP_TOPIC: [b'{"plain": "json"}']})
    struct = sink._struct(SP_TOPIC)
    assert "plain" in struct.names  # decode failed -> JSON fallback
