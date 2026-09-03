"""Fleet selftest: conformance-checks the wire protocol without a robot (spec §8)."""

import importlib
import json
import os
import pathlib
import queue
import sys
import threading
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
        spool.ack("channels", 2)

        pub = FakePublisher()
        run_selftest(pub, spool, batches=2, interval_s=0.0)

        assert [b["seq"] for b in pub.channel_calls] == [3, 4]

    def test_pending_channels_backlog_refuses_before_any_side_effect(
        self, tmp_path: pathlib.Path
    ) -> None:
        """C1 (critical): a paused service's queued-but-unsent channels backlog
        must never be silently advanced-past by the selftest's own acks."""
        from src.sink.publish.selftest import SelftestPreconditionError, run_selftest

        spool = Spool(tmp_path / "spool")
        spool.append("channels", 1, {"pre": 1})  # pending: never acked

        pub = FakePublisher()

        with pytest.raises(SelftestPreconditionError, match="channels"):
            run_selftest(pub, spool, batches=2, interval_s=0.0)

        # Nothing appended/acked/connected -- the precondition check runs
        # before any side effect.
        assert pub.calls == []
        assert list(spool.pending("channels")) == [(1, {"pre": 1})]
        assert list(spool.pending("events")) == []

    def test_pending_events_backlog_refuses_before_any_side_effect(
        self, tmp_path: pathlib.Path
    ) -> None:
        """C1 (critical): same ruling for the events lane."""
        from src.sink.publish.selftest import SelftestPreconditionError, run_selftest

        spool = Spool(tmp_path / "spool")
        spool.append("events", 1, {"pre": 1})  # pending: never acked

        pub = FakePublisher()

        with pytest.raises(SelftestPreconditionError, match="events"):
            run_selftest(pub, spool, batches=2, interval_s=0.0)

        assert pub.calls == []
        assert list(spool.pending("channels")) == []
        assert list(spool.pending("events")) == [(1, {"pre": 1})]

    def test_empty_lanes_runs_without_a_precondition_error(self, tmp_path: pathlib.Path) -> None:
        """C1: the precondition check must not false-positive on a fresh/fully-
        acked spool -- the common case."""
        from src.sink.publish.selftest import run_selftest

        spool = Spool(tmp_path / "spool")
        pub = FakePublisher()

        result = run_selftest(pub, spool, batches=1, interval_s=0.0)

        assert result["batches"] == 1


class TestExclusiveLockDuringRun:
    """P1b (Codex round 3): the whole run holds `spool.exclusive()`, so a
    concurrent writer on the same spool root either waits for it or the run
    refuses cleanly up front -- never an interleaved seq race mid-run."""

    def test_refuses_before_any_side_effect_when_another_writer_holds_the_lock(
        self, tmp_path: pathlib.Path
    ) -> None:
        from src.sink.publish.selftest import SelftestPreconditionError, run_selftest

        root = tmp_path / "spool"
        holder_spool = Spool(root)
        run_spool = Spool(root)
        holding = threading.Event()
        release = threading.Event()

        def hold_the_lock() -> None:
            with holder_spool.exclusive(timeout=1.0):
                holding.set()
                release.wait(timeout=2.0)

        t = threading.Thread(target=hold_the_lock)
        t.start()
        holding.wait(timeout=2.0)

        pub = FakePublisher()
        try:
            with pytest.raises(SelftestPreconditionError, match="another writer holds the spool"):
                run_selftest(pub, run_spool, batches=1, interval_s=0.0, lock_timeout_s=0.1)
        finally:
            release.set()
            t.join()

        # Refused before connecting or touching the spool at all.
        assert pub.calls == []
        assert list(run_spool.pending("channels")) == []

    def test_waits_out_a_briefly_held_lock_then_runs_normally(
        self, tmp_path: pathlib.Path
    ) -> None:
        from src.sink.publish.selftest import run_selftest

        root = tmp_path / "spool"
        holder_spool = Spool(root)
        run_spool = Spool(root)
        holding = threading.Event()

        def hold_briefly() -> None:
            with holder_spool.exclusive(timeout=1.0):
                holding.set()
                time.sleep(0.15)

        t = threading.Thread(target=hold_briefly)
        t.start()
        holding.wait(timeout=2.0)

        pub = FakePublisher()
        result = run_selftest(pub, run_spool, batches=1, interval_s=0.0, lock_timeout_s=2.0)
        t.join()

        assert result["batches"] == 1
        assert list(run_spool.pending("channels")) == []


class TestSelftestBetweenServiceAppendsDoesNotCauseADuplicate:
    """P1 follow-up (Codex round 3, PR #214): `exclusive()` (P1b) only
    serializes concurrent DISK ACCESS -- it does nothing to refresh a
    DIFFERENT already-open `Spool` instance's in-process seq cache. A
    `FleetService`'s long-lived `Spool` whose cache predates a selftest run
    against the same real spool must get a clean `ValueError` on its next
    append, never a silent duplicate seq."""

    def test_service_next_append_raises_cleanly_instead_of_duplicating(
        self, tmp_path: pathlib.Path
    ) -> None:
        from src.sink.publish.selftest import run_selftest

        root = tmp_path / "spool"

        # The "service": a long-lived Spool instance that has already
        # written and acked one channels record (so nothing is pending when
        # the selftest's precondition check runs against the same root).
        service_spool = Spool(root)
        service_spool.append("channels", 1, {"v": 1, "samples": []})
        service_spool.ack("channels", 1)

        # A selftest run happens on a SEPARATE Spool instance -- simulating
        # a different process (the CLI) against the SAME real spool root.
        selftest_spool = Spool(root)
        pub = FakePublisher()
        run_selftest(pub, selftest_spool, batches=2, interval_s=0.0)

        # The service's cache still says last_seq=1 -- it never saw the
        # selftest's writes. Its next append (seq=2) collides with what the
        # selftest already wrote to disk; this must raise cleanly, not
        # silently duplicate.
        with pytest.raises(ValueError, match="monotonic"):
            service_spool.append("channels", 2, {"v": 1, "samples": []})

        # Disk state is untouched by the rejected call, and the service
        # recovers cleanly via the disk-authoritative next_seq().
        assert list(service_spool.pending("channels")) == []
        correct_seq = service_spool.next_seq("channels")
        assert correct_seq > 2
        service_spool.append("channels", correct_seq, {"v": 1, "samples": []})


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

    def test_publisher_gets_the_selftest_client_id_suffix(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
    ) -> None:
        """Codex round 3 follow-up (PR #214, P2, comment 3925391258):
        without a distinct client id, the selftest's MqttPublisher would
        derive the SAME deterministic client id as the live service's own
        (same tenant/robot) -- the broker kicks the existing session when a
        new connection claims an already-connected client id, so running
        the selftest against an enrolled robot's broker while its real
        streaming service is connected would silently displace it."""
        import src.sink.publish.selftest as selftest_mod

        monkeypatch.setattr(settings, "FLEET_ENABLED", True)
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path / "identity"))
        monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))

        captured: dict = {}

        def fake_mqtt_publisher(*args: object, **kwargs: object) -> object:
            captured["kwargs"] = kwargs
            return object()

        monkeypatch.setattr(selftest_mod, "MqttPublisher", fake_mqtt_publisher)
        monkeypatch.setattr(
            selftest_mod,
            "run_selftest",
            lambda publisher, spool, **kwargs: {
                "channels": 4,
                "batches": 1,
                "samples": 4,
                "heartbeat": 1,
                "events": 1,
            },
        )

        rc = selftest_mod.main(
            ["--broker", "mqtt://localhost:1883", "--batches", "1", "--interval-s", "0"]
        )

        assert rc == 0
        assert captured["kwargs"]["client_id_suffix"] == "-selftest"

    def test_load_identity_or_none_is_a_single_load_not_check_then_load(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """M4 (mirrors control._load_identity_or_none's identical regression
        test): a corrupt/deleted identity must degrade to `None` via a single
        `load_identity()` call caught here, not a separate `is_enrolled()`
        check followed by a `load_identity()` call that could TOCTOU-race it.
        """
        import src.sink.publish.selftest as selftest_mod
        from src.sink.publish import FleetNotEnrolledError

        def _raise(*_a: object, **_kw: object) -> None:
            raise FleetNotEnrolledError("deleted between checks")

        monkeypatch.setattr(selftest_mod, "load_identity", _raise)

        assert selftest_mod._load_identity_or_none(pathlib.Path("/nonexistent")) is None


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
