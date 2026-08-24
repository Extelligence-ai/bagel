"""End-to-end tests for Copper (copper-rs) data via the app-side MCAP export.

Copper unified logs are decoded with the generating application's compile-time
types, so Bagel ingests Copper data through the app's `export-mcap` output:
JSON-encoded channels with jsonschema schemas, one channel per task plus a
`<task>/__meta` channel for iterations where that task produced no payload.

The committed sample was produced by a real Copper application (synthetic IMU
source -> filter -> sink, 100 iterations) and exported with
`probe-logreader logs/probe.copper export-mcap --output imu_probe.mcap`.
"""

import pathlib

import pyarrow as pa
import pytest

import server
from settings import settings
from src.di import module
from src.di.types import data_source

SAMPLE = "./data/sample/copper/imu_probe.mcap"

COPPER_MAGIC = b"\xb4\xa5\x50\xff"


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def test_copper_mcap_resolves_as_mcap() -> None:
    assert data_source.resolve(SAMPLE) == data_source.DataSource.MCAP


def test_copper_topics_and_schema() -> None:
    factory = module.provide("src.source.mcap", {"path": SAMPLE})
    registry = module.provide("src.topic.mcap", {})
    bag = factory.build()

    topics = registry.available_topics(bag)
    # One channel per task; empty per-iteration slots surface on `<task>/__meta`.
    assert "/imu" in topics
    assert "/filter" in topics

    assert registry.native_type_name("/imu", bag) == "copper.imu"

    struct = registry.struct("/imu", bag)
    # Copper wraps every message: payload + time-of-validity + process_time + status.
    payload = struct.field("payload").type
    assert payload.field("accel_x").type == pa.float64()
    assert payload.field("temperature_c").type == pa.float64()
    assert struct.field("tov").type.field("time_ns").type == pa.int64()

    filtered = registry.struct("/filter", bag).field("payload").type
    assert filtered.field("overheating").type == pa.bool_()


def test_copper_sql_end_to_end() -> None:
    rows = server.query_messages(
        path=SAMPLE,
        sql_statement=(
            "SELECT COUNT(*) AS n, "
            'MAX("/filter".payload.temperature_c) AS max_temp, '
            'SUM(CASE WHEN "/filter".payload.overheating THEN 1 ELSE 0 END) AS hot '
            'FROM "/filter"'
        ),
        topic="/filter",
    )
    assert rows[0]["n"] == 100
    # The sample ramps 35C -> ~45C over 100 iterations; never past the 70C threshold.
    assert 35.0 < rows[0]["max_temp"] < 70.0
    assert rows[0]["hot"] == 0


def test_raw_copper_log_gets_actionable_error(tmp_path: pathlib.Path) -> None:
    raw = tmp_path / "robot.copper"
    raw.write_bytes(COPPER_MAGIC + b"\x00" * 64)
    with pytest.raises(ValueError, match="export-mcap"):
        data_source.resolve(str(raw))


def test_json_newtype_scalars_wrap_into_single_field_structs(tmp_path: pathlib.Path) -> None:
    """Copper's unit newtypes trace as ``{"value": number}`` while serde emits the bare number.

    Bagel wraps such scalars into the single-field struct the schema declares
    instead of failing the whole channel (found on copper-rs's flight controller).
    """
    import json

    from mcap.writer import Writer

    path = tmp_path / "newtype.mcap"
    schema = {
        "type": "object",
        "properties": {
            "payload": {
                "type": "object",
                "properties": {
                    "altitude": {
                        "type": "object",
                        "properties": {"value": {"type": "number"}},
                    },
                    "pose": {
                        "type": "object",
                        "properties": {
                            "roll": {"type": "object", "properties": {"value": {"type": "number"}}}
                        },
                    },
                    "legs": {
                        "type": "array",
                        "items": {"type": "object", "properties": {"value": {"type": "number"}}},
                    },
                    "armed": {"type": "boolean"},
                },
            }
        },
    }
    with path.open("wb") as stream:
        writer = Writer(stream)
        writer.start()
        schema_id = writer.register_schema("copper.nav", "jsonschema", json.dumps(schema).encode())
        channel_id = writer.register_channel("/navigation", "json", schema_id)
        for i in range(3):
            message = {
                "payload": {
                    "altitude": 212.0 + i,
                    "pose": {"roll": -0.0},
                    "legs": [1.5, 2.5],
                    "armed": i > 0,
                }
            }
            writer.add_message(
                channel_id, i * 1_000_000, json.dumps(message).encode(), i * 1_000_000
            )
        writer.finish()

    rows = server.query_messages(
        path=str(path),
        topic="/navigation",
        sql_statement=(
            'SELECT MAX("/navigation".payload.altitude.value) AS alt, '
            'MIN("/navigation".payload.pose.roll.value) AS roll, '
            'SUM(CASE WHEN "/navigation".payload.armed THEN 1 ELSE 0 END) AS armed '
            'FROM "/navigation"'
        ),
    )
    assert rows == [{"alt": 214.0, "roll": 0.0, "armed": 2}]
