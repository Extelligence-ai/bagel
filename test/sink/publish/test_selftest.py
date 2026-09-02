"""Fleet selftest: conformance-checks the wire protocol without a robot (spec §8)."""

import importlib
import json
import os
import pathlib
import queue
import sys
import time
import uuid
from urllib.parse import urlparse

import pytest

from publish.conftest import FakePublisher
from settings import settings
from src.sink.publish.publisher import PublishError
from src.sink.publish.spool import Spool

BROKER = os.environ.get("MQTT_TEST_BROKER")

REQUIRED_EVENT_KEYS = {
    "v",
    "seq",
    "event_id",
    "name",
    "t_start",
    "t_end",
    "source_topic",
    "summary",
}


def test_selftest_module_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.selftest", raising=False)
    importlib.import_module("src.sink.publish.selftest")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )


class TestRunSelftest:
    def test_happy_path_call_order_and_payloads(self, tmp_path: pathlib.Path) -> None:
        from src.sink.publish.selftest import SELFTEST_CHANNELS, run_selftest

        pub = FakePublisher()
        spool = Spool(tmp_path / "spool")

        result = run_selftest(pub, spool, batches=5, interval_s=0.0, now=lambda: 1000.0)

        # -- call order: connect, schema first, then 5 channel batches, then
        # heartbeat, then one event, then close last.
        assert pub.calls == ["connect", "schema"] + ["channels"] * 5 + [
            "heartbeat",
            "events",
            "close",
        ]

        # -- schema payload is the fixed four-channel conformance schema.
        assert pub.schema_calls == [{"v": 1, "channels": SELFTEST_CHANNELS}]

        # -- batches: strictly monotonic seqs, deterministic values.
        seqs = [batch["seq"] for batch in pub.channel_calls]
        assert seqs == sorted(seqs)
        assert len(set(seqs)) == len(seqs)
        for i, batch in enumerate(pub.channel_calls):
            assert batch["v"] == 1
            assert batch["t_batch"] == 1000.0
            assert batch["samples"] == [
                {"c": "selftest.number", "t": 1000.0, "v": float(i)},
                {"c": "selftest.bool", "t": 1000.0, "v": i % 2 == 0},
                {"c": "selftest.string", "t": 1000.0, "v": f"selftest-{i}"},
                {
                    "c": "selftest.geo",
                    "t": 1000.0,
                    "v": {"lat": 52.0 + i * 0.001, "lon": 13.0 + i * 0.001},
                },
            ]

        # -- one retained heartbeat, with spool.evicted present.
        assert len(pub.heartbeat_calls) == 1
        assert "evicted" in pub.heartbeat_calls[0]["spool"]

        # -- one events payload with every §3 required key.
        assert len(pub.event_calls) == 1
        assert REQUIRED_EVENT_KEYS <= set(pub.event_calls[0])
        assert pub.event_calls[0]["name"] == "selftest"

        # -- close last, spool left with zero pending on both lanes (acks ran).
        assert pub.close_calls == 1
        assert list(spool.pending("channels")) == []
        assert list(spool.pending("events")) == []

        # -- return summary.
        assert result["channels"] == 4
        assert result["batches"] == 5
        assert result["heartbeat"] == 1
        assert result["events"] == 1
        assert result["channels_seq"] == [seqs[0], seqs[-1]]
        assert result["events_seq"] == pub.event_calls[0]["seq"]

        # -- "samples" counts what was actually published, not a formula:
        # pin it against the real per-batch sample counts captured on the
        # FakePublisher, not just the expected 5*4 total.
        total_captured_samples = sum(len(batch["samples"]) for batch in pub.channel_calls)
        assert result["samples"] == 5 * 4
        assert result["samples"] == total_captured_samples

    def test_cleanup_on_failure_acks_past_last_appended_seq(self, tmp_path: pathlib.Path) -> None:
        from src.sink.publish.selftest import run_selftest

        pub = FakePublisher(fail_at_channel_call=3)
        spool = Spool(tmp_path / "spool")

        with pytest.raises(PublishError):
            run_selftest(pub, spool, batches=5, interval_s=0.0)

        # Batches 1-2 fully acked already; batch 3's append must be cleaned
        # up (acked away) too -- nothing lingers for the real service.
        assert list(spool.pending("channels")) == []
        assert list(spool.pending("events")) == []
        # Never reached the heartbeat/events/close steps.
        assert pub.heartbeat_calls == []
        assert pub.event_calls == []
        assert pub.close_calls == 0

    def test_seqs_continue_from_preseeded_spool(self, tmp_path: pathlib.Path) -> None:
        from src.sink.publish.selftest import run_selftest

        spool = Spool(tmp_path / "spool")
        spool.append("channels", 1, {"pre": 1})
        spool.append("channels", 2, {"pre": 2})

        pub = FakePublisher()
        run_selftest(pub, spool, batches=2, interval_s=0.0)

        assert [b["seq"] for b in pub.channel_calls] == [3, 4]


class TestMain:
    def test_fleet_disabled_returns_1_no_publisher_constructed(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        import src.sink.publish.selftest as selftest_mod

        monkeypatch.setattr(settings, "FLEET_ENABLED", False)

        def _boom(*args: object, **kwargs: object) -> None:
            raise AssertionError("MqttPublisher must not be constructed")

        monkeypatch.setattr(selftest_mod, "MqttPublisher", _boom)

        rc = selftest_mod.main([])
        assert rc == 1
        err = capsys.readouterr().err
        assert "FLEET_ENABLED" in err

    def test_unenrolled_no_broker_returns_1(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
    ) -> None:
        import src.sink.publish.selftest as selftest_mod

        monkeypatch.setattr(settings, "FLEET_ENABLED", True)
        monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path / "identity"))

        rc = selftest_mod.main([])
        assert rc == 1
        err = capsys.readouterr().err
        assert err.strip() != ""

    def test_dev_broker_returns_0_and_prints_summary(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
    ) -> None:
        import src.sink.publish.selftest as selftest_mod

        monkeypatch.setattr(settings, "FLEET_ENABLED", True)
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path / "identity"))
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))

        sentinel = object()
        summary = {"channels": 4, "batches": 3, "samples": 12, "heartbeat": 1, "events": 1}
        captured: dict = {}

        def fake_mqtt_publisher(*args: object, **kwargs: object) -> object:
            captured["publisher_args"] = (args, kwargs)
            return sentinel

        def fake_run_selftest(publisher: object, spool: object, **kwargs: object) -> dict:
            captured["publisher"] = publisher
            captured["spool"] = spool
            captured["kwargs"] = kwargs
            return summary

        monkeypatch.setattr(selftest_mod, "MqttPublisher", fake_mqtt_publisher)
        monkeypatch.setattr(selftest_mod, "run_selftest", fake_run_selftest)

        rc = selftest_mod.main(
            ["--broker", "mqtt://localhost:1883", "--batches", "3", "--interval-s", "0"]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert json.loads(out) == summary
        assert captured["publisher"] is sentinel
        assert captured["kwargs"]["batches"] == 3
        assert captured["kwargs"]["interval_s"] == 0.0


# -- e2e: gated on a real broker, same idiom as test_mqtt_integration.py. --------


@pytest.mark.skipif(not BROKER, reason="MQTT_TEST_BROKER not set")
def test_selftest_e2e_round_trip(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    import paho.mqtt.client as paho

    import src.sink.publish.selftest as selftest_mod

    inbox: queue.Queue[tuple[str, bytes, bool]] = queue.Queue()
    client = paho.Client(
        callback_api_version=paho.CallbackAPIVersion.VERSION2,
        client_id=f"sub-{uuid.uuid4().hex[:8]}",
        protocol=paho.MQTTv5,
    )
    client.on_message = lambda cl, ud, msg: inbox.put((msg.topic, msg.payload, msg.retain))
    parsed = urlparse(BROKER)
    client.connect(parsed.hostname, parsed.port or 1883)
    client.loop_start()
    client.subscribe("bagel/v1/dev/robot/#", qos=1)
    time.sleep(0.3)

    monkeypatch.setattr(settings, "FLEET_ENABLED", True)
    monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
    monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path / "identity"))
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))

    try:
        rc = selftest_mod.main(["--broker", BROKER, "--batches", "3", "--interval-s", "0"])
        assert rc == 0

        got: dict[str, list[tuple[bytes, bool]]] = {}

        def _drain_all(timeout: float = 5.0) -> None:
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    topic, payload, retain = inbox.get(timeout=max(0.0, deadline - time.time()))
                except queue.Empty:
                    return
                got.setdefault(topic, []).append((payload, retain))

        _drain_all()

        schema_topic = "bagel/v1/dev/robot/schema"
        channels_topic = "bagel/v1/dev/robot/channels"
        heartbeat_topic = "bagel/v1/dev/robot/heartbeat"
        events_topic = "bagel/v1/dev/robot/events"

        assert schema_topic in got
        assert channels_topic in got and len(got[channels_topic]) == 3
        seqs = [json.loads(p)["seq"] for p, _ in got[channels_topic]]
        assert seqs == sorted(seqs)
        assert heartbeat_topic in got
        assert events_topic in got and len(got[events_topic]) == 1

        heartbeats = [json.loads(p) for p, _ in got[heartbeat_topic]]
        assert heartbeats[-1] == {
            "v": 1,
            "t": heartbeats[-1]["t"],
            "online": False,
            "reason": "stopped",
        }
    finally:
        client.loop_stop()
        client.disconnect()
