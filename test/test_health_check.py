"""check_robot_health (#218, #219): one call, a report card computed in SQL."""

import json
import math
import pathlib

import pytest
from mcap.writer import Writer

from bagel_mcp import server

EPOCH = 1_700_000_000.0
NS = 1_000_000_000
SCHEMAS = {
    "/battery": {"type": "object", "properties": {"voltage": {"type": "number"}}},
    "/imu": {
        "type": "object",
        "properties": {
            "linear_acceleration": {
                "type": "object",
                "properties": {k: {"type": "number"} for k in ("x", "y", "z")},
            }
        },
    },
    "/gps": {
        "type": "object",
        "properties": {"status": {"type": "object", "properties": {"status": {"type": "integer"}}}},
    },
}


def _write(path: pathlib.Path, faulty: bool) -> str:
    """60 s robot log: battery 10 Hz, IMU 50 Hz, GPS 5 Hz; `faulty` plants problems."""
    with open(path, "wb") as stream:
        writer = Writer(stream)
        writer.start()
        channels = {}
        for topic, schema in SCHEMAS.items():
            schema_id = writer.register_schema(
                name=topic.strip("/"), encoding="jsonschema", data=json.dumps(schema).encode()
            )
            channels[topic] = writer.register_channel(topic, "json", schema_id)

        def emit(topic: str, t: float, payload: dict) -> None:
            ns = int((EPOCH + t) * NS)
            writer.add_message(channels[topic], ns, json.dumps(payload).encode(), ns)

        for i in range(600):
            t = i / 10
            volts = 24.0 - 0.01 * i
            if faulty and t >= 30:
                volts -= 8.0  # pack collapse at t = 30 s
            emit("/battery", t, {"voltage": volts})
        for i in range(3000):
            t = i / 50
            if faulty and 40 <= t < 42:
                continue  # 2 s IMU dropout
            wobble = 0.05 * math.sin(i)
            if faulty and 20 <= t < 21:
                wobble *= 40  # vibration burst
            emit("/imu", t, {"linear_acceleration": {"x": wobble, "y": wobble, "z": 9.8}})
        for i in range(300):
            t = i / 5
            emit("/gps", t, {"status": {"status": -1 if faulty and t >= 10 else 0}})
        writer.finish()
    return str(path)


@pytest.fixture
def clean_log(tmp_path: pathlib.Path) -> str:
    return _write(tmp_path / "clean.mcap", faulty=False)


@pytest.fixture
def faulty_log(tmp_path: pathlib.Path) -> str:
    return _write(tmp_path / "faulty.mcap", faulty=True)


def test_clean_log_is_a_go(clean_log: str) -> None:
    report = server.check_robot_health(clean_log)
    statuses = {name: check["status"] for name, check in report["checks"].items()}
    assert statuses == {
        "power": "ok",
        "imu": "ok",
        "gps": "ok",
        "data_gaps": "ok",
        "errors": "skipped",  # MCAP JSON channels carry no diagnostic log
    }
    assert report["verdict"] == "go"


def test_faulty_log_flags_each_planted_problem(faulty_log: str) -> None:
    report = server.check_robot_health(faulty_log)
    checks = report["checks"]

    assert checks["power"]["status"] == "fail"
    assert checks["power"]["largest_drop"] == pytest.approx(8.0, abs=0.05)
    assert checks["power"]["largest_drop_at_seconds"] == pytest.approx(EPOCH + 30, abs=0.01)

    assert checks["imu"]["status"] == "warn"
    assert checks["imu"]["worst_burst"]["at_seconds"] == pytest.approx(EPOCH + 20, abs=1.0)

    assert checks["gps"]["status"] == "fail"
    assert checks["gps"]["weak_fix_share"] > 0.8

    assert "/imu" in checks["data_gaps"]["gaps"]
    assert checks["data_gaps"]["topics"]["/imu"]["largest_gap_seconds"] == pytest.approx(
        2.02, abs=0.01
    )
    assert report["verdict"] == "no-go"


def test_window_scopes_the_checks(faulty_log: str) -> None:
    # Before any planted fault except the vibration burst at t = 20 s.
    report = server.check_robot_health(faulty_log, start_seconds=EPOCH, end_seconds=EPOCH + 9)
    statuses = {name: check["status"] for name, check in report["checks"].items()}
    assert statuses["power"] == statuses["gps"] == statuses["data_gaps"] == "ok"


def test_last_seconds_is_the_live_preflight_window(faulty_log: str) -> None:
    report = server.check_robot_health(faulty_log, last_seconds=5)
    assert report["window_seconds"][1] - report["window_seconds"][0] == pytest.approx(5, abs=0.1)
    assert report["checks"]["gps"]["status"] == "fail"  # still no fix at the end
    assert report["checks"]["power"]["status"] == "ok"  # the collapse was 30 s earlier


def test_px4_sample_reports_takeoff_sag_and_its_log_levels() -> None:
    pytest.importorskip("pyulog")
    report = server.check_robot_health(
        "./data/sample/px4/sample.ulg", args={"download_description": False}
    )
    power = report["checks"]["power"]
    assert power["signal"] == "battery_status_0.voltage_v"  # the pack, not px4io's rail
    assert power["status"] == "warn"
    assert report["checks"]["gps"]["status"] == "skipped"
    assert report["checks"]["errors"]["levels"] == {"INFO": 5}


def test_ros_rosout_levels_are_named() -> None:
    report = server.check_robot_health("./data/sample/ros2/mcap")
    assert report["checks"]["errors"]["levels"] == {"INFO": 7}


def test_start_and_last_seconds_are_exclusive(clean_log: str) -> None:
    with pytest.raises(ValueError, match="not both"):
        server.check_robot_health(clean_log, start_seconds=EPOCH, last_seconds=5)
