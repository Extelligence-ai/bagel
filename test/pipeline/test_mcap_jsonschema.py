"""End-to-end tests for jsonschema-encoded MCAP topics -- ROS-free.

Writes an MCAP by hand (schema encoding "jsonschema", message encoding "json"),
the shape produced by Foxglove-native and custom JSON loggers, then drives
resolve -> topics -> struct -> SQL -> preview through the generic mcap modules.
"""

import json
import pathlib

import pyarrow as pa
import pytest
from mcap.writer import Writer

import server
from settings import settings
from src.di.types import data_source
from src.topic.mcap import jsonschema_to_struct

EPOCH = 1_700_000_000.0
SECOND_NS = 1_000_000_000

SCHEMA = {
    "type": "object",
    "properties": {
        "temp": {"type": "number"},
        "cycles": {"type": "integer"},
        "running": {"type": "boolean"},
        "mode": {"type": "string"},
        "gps": {
            "type": "object",
            "properties": {"lat": {"type": "number"}, "lon": {"type": "number"}},
        },
        "alarms": {"type": "array", "items": {"type": "string"}},
    },
}

RATE_HZ = 2
DURATION_SECONDS = 20.0
EVENTS = ((6.0, 8.0), (14.0, 15.0))  # offsets where temp > 30


def _temp_at(offset: float) -> float:
    return 34.0 if any(start <= offset <= end for start, end in EVENTS) else 22.0


@pytest.fixture
def jsonschema_mcap(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "iot.mcap"
    with open(path, "wb") as stream:
        writer = Writer(stream)
        writer.start()
        schema_id = writer.register_schema(
            name="factory.Reading",
            encoding="jsonschema",
            data=json.dumps(SCHEMA).encode(),
        )
        channel_id = writer.register_channel(
            topic="/readings", message_encoding="json", schema_id=schema_id
        )
        for i in range(int(DURATION_SECONDS * RATE_HZ)):
            offset = i / RATE_HZ
            timestamp_ns = int((EPOCH + offset) * SECOND_NS)
            payload = {
                "temp": _temp_at(offset),
                "cycles": i,
                "running": True,
                "mode": "auto",
                "gps": {"lat": 37.77, "lon": -122.42},
                "alarms": [] if _temp_at(offset) < 30 else ["overtemp"],
            }
            writer.add_message(
                channel_id=channel_id,
                log_time=timestamp_ns,
                data=json.dumps(payload).encode(),
                publish_time=timestamp_ns,
            )
        writer.finish()
    return path


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def test_jsonschema_mapper_types() -> None:
    struct = jsonschema_to_struct(json.dumps(SCHEMA).encode())
    assert struct.field("temp").type == pa.float64()
    assert struct.field("cycles").type == pa.int64()
    assert struct.field("running").type == pa.bool_()
    assert struct.field("mode").type == pa.string()
    assert pa.types.is_struct(struct.field("gps").type)
    assert pa.types.is_list(struct.field("alarms").type)


def test_jsonschema_mapper_edge_cases() -> None:
    struct = jsonschema_to_struct(
        json.dumps(
            {
                "type": "object",
                "properties": {
                    "maybe": {"type": ["number", "null"]},
                    "anything": {},
                    "blob": {"type": "object"},
                },
            }
        ).encode()
    )
    assert struct.field("maybe").type == pa.float64()
    assert struct.field("anything").type == pa.string()
    assert struct.field("blob").type == pa.string()


def test_end_to_end_jsonschema_mcap(jsonschema_mcap: pathlib.Path) -> None:
    assert data_source.resolve(str(jsonschema_mcap)) == data_source.DataSource.MCAP

    from src.di import module

    factory = module.provide("src.source.mcap", {"path": str(jsonschema_mcap)})
    registry = module.provide("src.topic.mcap", {})
    bag = factory.build()

    assert registry.available_topics(bag) == ["/readings"]
    assert registry.native_type_name("/readings", bag) == "factory.Reading"
    struct = registry.struct("/readings", bag)
    assert struct.field("temp").type == pa.float64()
    assert "temp" in registry.describe("/readings", bag)

    rows = server.query_messages(
        path=str(jsonschema_mcap),
        sql_statement=(
            "SELECT COUNT(*) AS n, MAX(\"/readings\"['temp']) AS max_temp, "
            "MAX(\"/readings\"['gps']['lat']) AS lat FROM \"/readings\""
        ),
        topic="/readings",
    )
    assert rows[0]["n"] == int(DURATION_SECONDS * RATE_HZ)
    assert rows[0]["max_temp"] == 34.0
    assert rows[0]["lat"] == pytest.approx(37.77)


def test_preview_detects_events_in_jsonschema_mcap(jsonschema_mcap: pathlib.Path) -> None:
    result = server.preview_pipeline(
        path=str(jsonschema_mcap),
        event_topic="/readings",
        predicate="\"/readings\"['temp'] > 30",
        pre_seconds=1.0,
        post_seconds=1.0,
        debounce_seconds=3.0,
    )
    assert result["event_count"] == len(EVENTS)
    assert 0 < result["kept_fraction"] < 1
