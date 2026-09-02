"""Tests for Task 4: first-boot enrollment hook + startup `streams:` wiring.

Reuses test_startup.py's FakePahoClient/monkeypatch conventions for the
subscribing sink side, and writes fleet identities directly to disk in
Task 1's on-disk layout (key/cert/ca + identity.yaml) rather than standing
up the fake enroll HTTP server -- these tests only need `is_enrolled()` /
`load_identity()` to see a complete identity, not a real enrollment round
trip (that's covered by test/sink/publish/test_identity.py).
"""

import itertools
import pathlib

import pytest

pytest.importorskip("paho")

import yaml
from conftest import FakePahoClient

from settings import settings
from src.sink import base as sink_base
from src.sink import mqtt as sink_mqtt
from src.sink import startup
from src.sink.publish import EnrollmentError
from src.sink.publish import identity as identity_mod
from src.sink.publish.service import FleetService

_PORT_COUNTER = itertools.count(31000)


def _write_identity(  # noqa: PLR0913 -- one field per identity.yaml key, matching enroll()'s own shape
    directory: pathlib.Path,
    *,
    tenant: str = "acme",
    robot_id: str = "robot-1",
    broker_url: str = "mqtts://fleet.example.com:8883",
    enroll_url: str = "https://enroll.example.com",
    expires_at: str = "2030-01-01T00:00:00Z",
) -> None:
    """Write a complete, `is_enrolled()`-satisfying identity (Task 1's fixed-name layout)."""
    directory.mkdir(parents=True, exist_ok=True)
    key_path = directory / "robot.key"
    key_path.write_bytes(b"fake-key-material")
    key_path.chmod(0o600)
    (directory / "robot.crt").write_bytes(b"fake-cert-material")
    (directory / "ca.crt").write_bytes(b"fake-ca-material")
    (directory / "identity.yaml").write_text(
        yaml.safe_dump(
            {
                "tenant": tenant,
                "robot_id": robot_id,
                "broker_url": broker_url,
                "enroll_url": enroll_url,
                "expires_at": expires_at,
            }
        )
    )


@pytest.fixture(autouse=True)
def _isolated_settings(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path / "cache"))
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path / "identity"))
    monkeypatch.setattr(settings, "FLEET_ENABLED", True)
    monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", False)
    monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", None)
    monkeypatch.setattr(settings, "FLEET_ENROLL_URL", None)
    sink_base._global_sink_singletons.clear()
    startup._FLEET_SERVICE = None
    yield
    if startup._FLEET_SERVICE is not None:
        startup._FLEET_SERVICE.stop()
        startup._FLEET_SERVICE = None
    sink_base._global_sink_singletons.clear()


def _subscription_entry(host: str, topic: str) -> dict:
    """One `subscriptions:` manifest entry subscribing to a single topic."""
    return {
        "sink": "mqtt",
        "host": host,
        "port": next(_PORT_COUNTER),
        "args": {"discovery_seconds": 0.0, "timestamp_field": "t"},
        "topics": [topic],
    }


def _write_manifest(
    tmp_path: pathlib.Path, manifest: dict, name: str = "startup.yaml"
) -> pathlib.Path:
    manifest_file = tmp_path / name
    manifest_file.write_text(yaml.safe_dump(manifest))
    return manifest_file


class TestFleetStartedReport:
    def test_started_when_identity_enrolled_and_topics_covered_by_one_entry(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        # The router's background thread will try a real connect() in a daemon
        # thread once FleetService.start() launches it; no-op it so tests never
        # touch the network (host `fleet.example.com` is a placeholder, not a
        # real broker).
        monkeypatch.setattr(startup.MqttPublisher, "connect", lambda self: None)

        fake = FakePahoClient()
        fake.retained = {"robot/telemetry": [b'{"speed": 1.5, "t": 100.0}']}
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry = _subscription_entry("manifest.test", "robot/telemetry")
        manifest = {
            "subscriptions": [entry],
            "streams": {
                "channels": [{"topic": "robot/telemetry", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports[0] == {
            "sink": "mqtt",
            "status": "subscribed",
            "topics": ["robot/telemetry"],
        }
        assert reports[1] == {"fleet": "started"}
        assert startup._FLEET_SERVICE is not None

        # Taps wired: the fleet service must have attached its queue tap to
        # the subscribed topic's buffer.
        buffer = startup._FLEET_SERVICE._sink._buffers["robot/telemetry"]
        assert buffer._tap is not None

    def test_dev_insecure_mqtt_broker_starts_without_identity(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        monkeypatch.setattr(startup.MqttPublisher, "connect", lambda self: None)

        fake = FakePahoClient()
        fake.retained = {"robot/telemetry": [b'{"speed": 1.5, "t": 100.0}']}
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry = _subscription_entry("manifest.test", "robot/telemetry")
        manifest = {
            "subscriptions": [entry],
            "streams": {
                "broker": "mqtt://127.0.0.1:1883",
                "channels": [{"topic": "robot/telemetry", "fields": ["speed"], "rate_hz": 1}],
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports[-1] == {"fleet": "started"}
        assert startup._FLEET_SERVICE is not None
        assert startup._FLEET_SERVICE._identity is None


class TestFleetServiceReplacement:
    def test_second_start_stops_the_previous_fleet_service(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Reproduces a second `startup.start()` in the same process (e.g. a
        # re-applied manifest): the previous FleetService must be stopped --
        # not merely dropped -- before the holder is replaced.
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        monkeypatch.setattr(startup.MqttPublisher, "connect", lambda self: None)

        # Spy on close() (rather than trusting `.connected`, which is
        # already False for a publisher whose connect() was no-op'd) to
        # prove the orphaned service's publisher was actually torn down.
        closed_publishers: list[object] = []
        real_close = startup.MqttPublisher.close

        def spy_close(self: object) -> None:
            closed_publishers.append(self)
            real_close(self)

        monkeypatch.setattr(startup.MqttPublisher, "close", spy_close)

        fake = FakePahoClient()
        fake.retained = {
            "robot/telemetry_1": [b'{"speed": 1.5, "t": 100.0}'],
            "robot/telemetry_2": [b'{"speed": 2.5, "t": 100.0}'],
        }
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry_1 = _subscription_entry("manifest.test", "robot/telemetry_1")
        manifest_1 = {
            "subscriptions": [entry_1],
            "streams": {
                "channels": [{"topic": "robot/telemetry_1", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file_1 = _write_manifest(tmp_path, manifest_1, name="startup1.yaml")

        reports_1 = startup.start(manifest_file_1)
        assert reports_1[-1] == {"fleet": "started"}
        service_1 = startup._FLEET_SERVICE
        assert service_1 is not None
        assert service_1._started is True
        assert service_1._router is not None
        assert service_1._router.alive is True
        assert service_1._heartbeat is not None
        assert service_1._heartbeat.alive is True

        entry_2 = _subscription_entry("manifest.test", "robot/telemetry_2")
        manifest_2 = {
            "subscriptions": [entry_2],
            "streams": {
                "channels": [{"topic": "robot/telemetry_2", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file_2 = _write_manifest(tmp_path, manifest_2, name="startup2.yaml")

        reports_2 = startup.start(manifest_file_2)  # must not raise

        assert reports_2[-1] == {"fleet": "started"}
        service_2 = startup._FLEET_SERVICE
        assert service_2 is not None
        assert service_2 is not service_1

        # The orphaned first service was actually stopped: not started,
        # its threads dead, its publisher closed -- not just replaced in
        # the holder and abandoned still running.
        assert service_1._started is False
        assert service_1._router.alive is False
        assert service_1._heartbeat.alive is False
        assert service_1._publisher.connected is False
        assert service_1._publisher in closed_publishers

        # The second service is the one actually live.
        assert service_2._started is True


class TestFleetServiceReplacementFailure:
    """MINOR fix: a failed second start must not leave `_FLEET_SERVICE`
    pointing at the FIRST service's now-stopped instance -- a caller reading
    the holder (step 7's status/control tools) would see a dead service as
    if it were live.
    """

    def test_new_start_failure_after_stopping_old_leaves_fleet_service_none(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        monkeypatch.setattr(startup.MqttPublisher, "connect", lambda self: None)

        fake = FakePahoClient()
        fake.retained = {
            "robot/telemetry_1": [b'{"speed": 1.5, "t": 100.0}'],
            "robot/telemetry_2": [b'{"speed": 2.5, "t": 100.0}'],
        }
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry_1 = _subscription_entry("manifest.test", "robot/telemetry_1")
        manifest_1 = {
            "subscriptions": [entry_1],
            "streams": {
                "channels": [{"topic": "robot/telemetry_1", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file_1 = _write_manifest(tmp_path, manifest_1, name="startup1.yaml")

        reports_1 = startup.start(manifest_file_1)
        assert reports_1[-1] == {"fleet": "started"}
        service_1 = startup._FLEET_SERVICE
        assert service_1 is not None

        # The second start's FleetService.start() fails AFTER the first
        # service has already been stopped and replaced.
        def failing_start(self: FleetService) -> None:
            raise RuntimeError("simulated start failure")

        monkeypatch.setattr(FleetService, "start", failing_start)

        entry_2 = _subscription_entry("manifest.test", "robot/telemetry_2")
        manifest_2 = {
            "subscriptions": [entry_2],
            "streams": {
                "channels": [{"topic": "robot/telemetry_2", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file_2 = _write_manifest(tmp_path, manifest_2, name="startup2.yaml")

        reports_2 = startup.start(manifest_file_2)  # must not raise

        assert reports_2[-1]["fleet"] == "failed"
        # BUG (pre-fix): _FLEET_SERVICE still pointed at the stopped, dead
        # service_1 -- a caller reading the holder would see a dead service
        # as if it were the live one.
        assert startup._FLEET_SERVICE is None


class TestFleetDisabled:
    def test_fleet_enabled_false_reports_disabled(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

        fake = FakePahoClient()
        fake.retained = {"robot/telemetry": [b'{"speed": 1.5, "t": 100.0}']}
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry = _subscription_entry("manifest.test", "robot/telemetry")
        manifest = {
            "subscriptions": [entry],
            "streams": {
                "channels": [{"topic": "robot/telemetry", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports[-1] == {"fleet": "disabled"}
        assert startup._FLEET_SERVICE is None


class TestFleetFailures:
    def test_mqtts_broker_with_no_identity_fails_with_not_enrolled_error(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # FLEET_IDENTITY_DIRECTORY is isolated to an empty tmp dir by the
        # autouse fixture -- nothing has enrolled here.
        fake = FakePahoClient()
        fake.retained = {"robot/telemetry": [b'{"speed": 1.5, "t": 100.0}']}
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry = _subscription_entry("manifest.test", "robot/telemetry")
        manifest = {
            "subscriptions": [entry],
            "streams": {
                "broker": "mqtts://fleet.example.com:8883",
                "channels": [{"topic": "robot/telemetry", "fields": ["speed"], "rate_hz": 1}],
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports[-1]["fleet"] == "failed"
        assert "enrolled fleet identity" in reports[-1]["error"]
        assert startup._FLEET_SERVICE is None

    def test_topics_spanning_two_subscription_entries_fails_with_v1_limitation(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

        fake = FakePahoClient()
        fake.retained = {
            "robot/telemetry_a": [b'{"speed": 1.5, "t": 100.0}'],
            "robot/telemetry_b": [b'{"battery": 90.0, "t": 100.0}'],
        }
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry_a = _subscription_entry("manifest.test", "robot/telemetry_a")
        entry_b = _subscription_entry("manifest.test", "robot/telemetry_b")
        manifest = {
            "subscriptions": [entry_a, entry_b],
            "streams": {
                "channels": [
                    {"topic": "robot/telemetry_a", "fields": ["speed"], "rate_hz": 1},
                    {"topic": "robot/telemetry_b", "fields": ["battery"], "rate_hz": 1},
                ]
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports[-1]["fleet"] == "failed"
        error = reports[-1]["error"]
        assert "single" in error.lower()
        assert "robot/telemetry_a" in error
        assert "robot/telemetry_b" in error
        assert startup._FLEET_SERVICE is None

    def test_topics_split_across_two_same_sink_entries_coverage_passes(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex review (3909413506): TopicSink.__new__ is a (host, port)
        # singleton -- two `subscriptions:` entries pointed at the SAME
        # host/port get the SAME sink object, with each entry's topics list
        # only naming what THAT entry subscribed. _find_covering_sink used
        # to test each (sink, topics) tuple independently, so a manifest
        # whose needed topics were split across two same-sink entries was
        # wrongly rejected even though the singleton sink is actually
        # subscribed to their union. RULING A's own docstring assumes this
        # would work ("each entry's sink is a fresh instance"), which the
        # singleton falsifies -- this is the real gap the ruling didn't
        # intend, not the genuine two-different-sinks v1 limitation covered
        # by test_topics_spanning_two_subscription_entries_fails_with_v1_limitation.
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        monkeypatch.setattr(startup.MqttPublisher, "connect", lambda self: None)

        fake = FakePahoClient()
        fake.retained = {
            "robot/telemetry_a": [b'{"speed": 1.5, "t": 100.0}'],
            "robot/telemetry_b": [b'{"battery": 90.0, "t": 100.0}'],
        }
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        same_port = next(_PORT_COUNTER)
        entry_a = {
            "sink": "mqtt",
            "host": "manifest.test",
            "port": same_port,
            "args": {"discovery_seconds": 0.0, "timestamp_field": "t"},
            "topics": ["robot/telemetry_a"],
        }
        entry_b = {
            "sink": "mqtt",
            "host": "manifest.test",
            "port": same_port,
            "args": {"discovery_seconds": 0.0, "timestamp_field": "t"},
            "topics": ["robot/telemetry_b"],
        }
        manifest = {
            "subscriptions": [entry_a, entry_b],
            "streams": {
                "channels": [
                    {"topic": "robot/telemetry_a", "fields": ["speed"], "rate_hz": 1},
                    {"topic": "robot/telemetry_b", "fields": ["battery"], "rate_hz": 1},
                ]
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports[-1] == {"fleet": "started"}
        assert startup._FLEET_SERVICE is not None

    def test_no_subscription_covers_streams_topics_fails(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

        manifest = {
            "streams": {
                "channels": [{"topic": "robot/telemetry", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports == [{"fleet": "failed", "error": reports[0]["error"]}]
        assert "robot/telemetry" in reports[0]["error"]
        assert startup._FLEET_SERVICE is None


class TestSinkCloseStopsFleetService:
    """Task 3: `TopicSink.close()` -> `FleetService.stop()` coupling (spec §2; step 6's
    ruling B retired) -- wired via `base.register_close_hook`."""

    def test_closing_the_fleet_sink_stops_the_service(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        monkeypatch.setattr(startup.MqttPublisher, "connect", lambda self: None)

        closed_publishers: list[object] = []
        real_close = startup.MqttPublisher.close

        def spy_close(self: object) -> None:
            closed_publishers.append(self)
            real_close(self)

        monkeypatch.setattr(startup.MqttPublisher, "close", spy_close)

        fake = FakePahoClient()
        fake.retained = {"robot/telemetry": [b'{"speed": 1.5, "t": 100.0}']}
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry = _subscription_entry("manifest.test", "robot/telemetry")
        manifest = {
            "subscriptions": [entry],
            "streams": {
                "channels": [{"topic": "robot/telemetry", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)
        assert reports[-1] == {"fleet": "started"}
        service = startup.fleet_service()
        assert service is not None
        sink = service.sink

        sink.close()

        assert startup.fleet_service() is None
        assert service._publisher in closed_publishers

    def test_closing_an_unrelated_sink_leaves_the_service_running(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        monkeypatch.setattr(startup.MqttPublisher, "connect", lambda self: None)

        fake = FakePahoClient()
        fake.retained = {"robot/telemetry": [b'{"speed": 1.5, "t": 100.0}']}
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry = _subscription_entry("manifest.test", "robot/telemetry")
        manifest = {
            "subscriptions": [entry],
            "streams": {
                "channels": [{"topic": "robot/telemetry", "fields": ["speed"], "rate_hz": 1}]
            },
        }
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)
        assert reports[-1] == {"fleet": "started"}
        service = startup.fleet_service()
        assert service is not None

        other_fake = FakePahoClient()
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: other_fake)
        unrelated_sink = sink_mqtt.TopicSink(
            host="unrelated.test",
            port=next(_PORT_COUNTER),
            discovery_seconds=0.0,
            timestamp_field="t",
        )
        try:
            unrelated_sink.close()

            assert startup.fleet_service() is service
            assert service._started is True
        finally:
            service.stop()


class TestNoStreamsKey:
    def test_manifest_without_streams_key_produces_no_fleet_report_entry(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = FakePahoClient()
        fake.retained = {"robot/telemetry": [b'{"speed": 1.5, "t": 100.0}']}
        monkeypatch.setattr(sink_mqtt.paho, "Client", lambda **_: fake)

        entry = _subscription_entry("manifest.test", "robot/telemetry")
        manifest = {"subscriptions": [entry]}
        manifest_file = _write_manifest(tmp_path, manifest)

        reports = startup.start(manifest_file)

        assert reports == [{"sink": "mqtt", "status": "subscribed", "topics": ["robot/telemetry"]}]
        assert all("fleet" not in report for report in reports)
        assert startup._FLEET_SERVICE is None


class TestFirstBootEnrollHook:
    TOKEN = "first-boot-secret-token"  # noqa: S105 -- test fixture token, not a real secret

    def test_enrolls_when_token_and_url_set_and_not_enrolled(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
        monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", self.TOKEN)
        monkeypatch.setattr(settings, "FLEET_ENROLL_URL", "https://enroll.example.com")

        calls = []

        def fake_enroll(token: str, url: str, dir_: pathlib.Path) -> identity_mod.Identity:
            calls.append((token, url, dir_))
            _write_identity(pathlib.Path(dir_))
            return identity_mod.load_identity(dir_)

        monkeypatch.setattr(identity_mod, "enroll", fake_enroll)

        identity_mod.maybe_enroll_on_first_boot()

        assert calls == [(self.TOKEN, "https://enroll.example.com", directory)]
        # Startup's fleet wiring must find the fresh identity right after.
        assert identity_mod.is_enrolled(directory) is True

    def test_skips_when_already_enrolled(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
        _write_identity(directory)
        monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", self.TOKEN)
        monkeypatch.setattr(settings, "FLEET_ENROLL_URL", "https://enroll.example.com")

        def fail_enroll(*_a: object, **_kw: object) -> None:
            raise AssertionError("enroll() must not be called when already enrolled")

        monkeypatch.setattr(identity_mod, "enroll", fail_enroll)

        identity_mod.maybe_enroll_on_first_boot()  # must not raise

    def test_skips_when_token_or_url_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fail_enroll(*_a: object, **_kw: object) -> None:
            raise AssertionError("enroll() must not be called with no token/url configured")

        monkeypatch.setattr(identity_mod, "enroll", fail_enroll)

        identity_mod.maybe_enroll_on_first_boot()  # FLEET_ENROLL_TOKEN/_URL are None -- no-op

    def test_enrollment_failure_logs_error_and_server_continues(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
        monkeypatch.setattr(settings, "FLEET_ENROLL_TOKEN", self.TOKEN)
        monkeypatch.setattr(settings, "FLEET_ENROLL_URL", "https://enroll.example.com")

        def failing_enroll(*_a: object, **_kw: object) -> identity_mod.Identity:
            raise EnrollmentError(401, "unknown token")

        monkeypatch.setattr(identity_mod, "enroll", failing_enroll)

        import logging

        with caplog.at_level(logging.DEBUG):
            identity_mod.maybe_enroll_on_first_boot()  # must not raise

        assert identity_mod.is_enrolled(directory) is False
        assert any(
            "First-boot fleet enrollment failed" in record.getMessage() for record in caplog.records
        )
        for record in caplog.records:
            assert self.TOKEN not in record.getMessage()
