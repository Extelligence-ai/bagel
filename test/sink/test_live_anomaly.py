"""The Jev anomaly gate on a live MQTT stream: ingest never waits on Jev."""

import json
import math
import pathlib
import time

import pytest

pytest.importorskip("paho")

from conftest import MakeSink

from settings import settings
from src.sink import startup
from test._fixtures.decision_server import DecisionServer, decision_server, jev_reply

server = decision_server
TOPIC = "motor/current"
JEV_SECONDS = 1.0
SPIKE = (101.0, 103.0)


def _current(t: float) -> float:
    return 9.0 if SPIKE[0] <= t <= SPIKE[1] else 1.0 + 0.05 * math.sin(t * 3.1)


def _slow_label(body: dict) -> tuple[int, dict]:
    time.sleep(JEV_SECONDS)
    choices = body["questions"]["decision"]["criteria"]
    other = 0.1 / (len(choices) - 1)
    return jev_reply({c: (0.9 if c == "overcurrent" else other) for c in choices})


def _pipeline(url: str) -> dict:
    return {
        "name": "live_anomaly",
        "site": "plant",
        "asset": "pump",
        "allow_failure": True,
        "cadence": {"topic": TOPIC, "when": {"every": 10, "unit": "second"}},
        "gates": [
            {
                "module": "src.pipeline.gates.anomaly",
                "lookback": {"last": 10, "unit": "second"},
                "args": {
                    "anomalies": {"overcurrent": "motor current far above normal"},
                    "signals": [f"{TOPIC}.value"],
                    "url": url,
                    "baseline_window_minutes": 5,
                    "warmup_minutes": 1,
                    "timeout_seconds": 5,
                },
            }
        ],
        "tasks": [{"module": "src.pipeline.tasks.write_annotations"}],
    }


@pytest.fixture(autouse=True)
def _isolated(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-key")


def test_a_live_spike_is_labelled_without_stalling_ingest(
    make_sink: MakeSink, server: DecisionServer
) -> None:
    server.reply = _slow_label
    sink = make_sink(
        retained={TOPIC: [json.dumps({"value": 1.0, "t": 0.0}).encode()]},
        timestamp_field="t",
    )
    startup.subscribe_with_pipeline(sink, [TOPIC], _pipeline(server.url))

    slowest = 0.0
    for tenth in range(1, 1301):  # 10 Hz for 130 s, the spike at 101-103 s
        t = tenth / 10
        payload = json.dumps({"value": _current(t), "t": t}).encode()
        started = time.monotonic()
        sink._fake.deliver(TOPIC, payload)
        slowest = max(slowest, time.monotonic() - started)
    sink.close()  # drains the worker

    assert server.requests, "the spike window must reach Jev"
    assert slowest < JEV_SECONDS / 2, f"a delivery waited {slowest:.2f} s on Jev"
    labels = [
        json.loads(path.read_text())["anomaly"]
        for path in pathlib.Path(settings.ARTIFACT_DIRECTORY).rglob("*.json")
    ]
    assert [label["label"] for label in labels] == ["overcurrent"]
    assert labels[0]["verified"] is True
