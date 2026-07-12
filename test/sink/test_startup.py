"""Tests for standing pipelines: startup manifest + pipeline-attached subscriptions."""

import json
import pathlib

import pytest

pytest.importorskip("paho")

import yaml
from conftest import FakePahoClient

from settings import settings
from src.sink import mqtt, startup

PIPELINE = {
    "name": "freezer_excursion",
    "site": "warehouse",
    "asset": "freezer_1",
    "allow_failure": True,
    "cadence": {
        "topic": "freezer/1/status",
        "when": {"on_event": {"predicate": "\"freezer/1/status\"['temp'] > -15"}},
    },
    "tasks": [
        {
            "module": "src.pipeline.tasks.write_topics_to_file",
            "args": {"topics": ["freezer/1/status"], "output_format": "csv"},
            "lookback": {"last": 60, "unit": "second"},
        }
    ],
}


@pytest.fixture(autouse=True)
def _isolated_dirs(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path / "cache"))
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def test_standing_pipeline_fires_and_writes_artifact(make_sink) -> None:  # noqa: ANN001
    sink = make_sink(
        retained={"freezer/1/status": [b'{"temp": -18.5, "t": 100.0}']},
        timestamp_field="t",
    )

    topics = startup.subscribe_with_pipeline(sink, ["freezer/1/status"], PIPELINE)
    assert topics == ["freezer/1/status"]

    # Cruise, then an excursion: the rising edge fires the standing pipeline.
    for t, temp in ((101.0, -18.0), (102.0, -12.0)):
        sink._fake.deliver(
            "freezer/1/status", json.dumps({"temp": temp, "t": t}).encode()
        )

    artifacts = list(pathlib.Path(settings.ARTIFACT_DIRECTORY).rglob("*.csv"))
    assert len(artifacts) == 1, "the excursion must produce exactly one snapshot"
    assert "pipeline=freezer_excursion" in str(artifacts[0])
    content = artifacts[0].read_text()
    assert "-12.0" in content, "snapshot must contain the excursion reading"


def test_pipeline_topic_must_be_subscribed(make_sink) -> None:  # noqa: ANN001
    sink = make_sink(retained={"plant/pump": [b'{"v": 1}']})
    with pytest.raises(ValueError, match="cadence topic"):
        startup.subscribe_with_pipeline(sink, ["plant/pump"], PIPELINE)


def test_manifest_startup_end_to_end(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.sink import base as sink_base

    sink_base._global_sink_singletons.clear()
    fake = FakePahoClient()
    fake.retained = {"freezer/1/status": [b'{"temp": -18.5, "t": 100.0}']}
    monkeypatch.setattr(mqtt.paho, "Client", lambda **_: fake)

    manifest = {
        "subscriptions": [
            {
                "sink": "mqtt",
                "host": "manifest.test",
                "port": 29999,
                "args": {"discovery_seconds": 0.0, "timestamp_field": "t"},
                "topics": ["freezer/1/status"],
                "pipeline": PIPELINE,
            }
        ]
    }
    manifest_file = tmp_path / "startup.yaml"
    manifest_file.write_text(yaml.safe_dump(manifest))

    reports = startup.start(manifest_file)
    assert reports == [
        {"sink": "mqtt", "status": "subscribed", "topics": ["freezer/1/status"]}
    ]

    # The subscription is live: an excursion fires the pipeline from the manifest.
    fake.deliver("freezer/1/status", json.dumps({"temp": -12.0, "t": 101.0}).encode())
    artifacts = list(pathlib.Path(settings.ARTIFACT_DIRECTORY).rglob("*.csv"))
    assert len(artifacts) == 1
    sink_base._global_sink_singletons.clear()


def test_manifest_isolates_failures(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FailingPahoClient(FakePahoClient):
        def connect(self, host: str, port: int, keepalive: int = 60) -> None:
            raise OSError("broker down")

    monkeypatch.setattr(mqtt.paho, "Client", lambda **_: FailingPahoClient())

    manifest = {
        "subscriptions": [
            {"sink": "mqtt", "host": "down.test", "port": 29998, "topics": ["x"]},
        ]
    }
    manifest_file = tmp_path / "startup.yaml"
    manifest_file.write_text(yaml.safe_dump(manifest))

    # Connection failure is reported instead of raising -- a dead broker
    # must not prevent server boot.
    reports = startup.start(manifest_file)
    assert reports[0]["status"] == "failed"
    assert "error" in reports[0]


def test_empty_manifest_is_fine(tmp_path: pathlib.Path) -> None:
    manifest_file = tmp_path / "startup.yaml"
    manifest_file.write_text("")
    assert startup.start(manifest_file) == []
