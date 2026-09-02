"""control.py: status/pause/resume lifecycle control (fleet streaming step 7, Task 4).

`fleet_status()` calls NO gate -- it is the local source of truth in every
state (uninstalled, disabled, unenrolled, service down); `pause_streaming`/
`resume_streaming` call `require_fleet()` first, so `FLEET_ENABLED=0` raises
before the holder is ever touched.
"""

import pathlib
import sys

import pytest
import yaml

from publish.conftest import FakePublisher, FakeSink, FakeWriter, _imu_streams, _imu_struct
from settings import settings
from src.sink import startup
from src.sink.publish import FleetDisabledError, FleetNotInstalledError, control
from src.sink.publish.service import FleetService
from src.sink.publish.spool import Spool


def _simulate_paho_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make both `find_spec("paho.mqtt")` and `import paho.mqtt.client` behave as uninstalled.

    Blanking `sys.modules["paho.mqtt"]` alone is enough for
    `importlib.util.find_spec("paho.mqtt")` (`fleet_status()`'s check) to
    return `None` -- but `require_fleet()`'s plain `import paho.mqtt.client`
    statement resolves straight from `sys.modules["paho.mqtt.client"]` if
    THAT'S already cached (e.g. by an earlier test in this same session that
    really imported paho), bypassing the parent's `None` entirely. Both keys
    need blanking together for a reliable "not installed" simulation
    regardless of what an earlier test already imported -- this is the same
    idiom `test_gate.py` uses (it blanks all three of `paho`/`paho.mqtt`/
    `paho.mqtt.client`; `paho` itself is left alone here since neither check
    this module makes ever resolves through it).
    """
    monkeypatch.setitem(sys.modules, "paho.mqtt", None)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", None)


def _write_identity(  # noqa: PLR0913 -- one field per identity.yaml key, matching enroll()'s own shape
    directory: pathlib.Path,
    *,
    tenant: str = "acme",
    robot_id: str = "robot-1",
    broker_url: str = "mqtts://fleet.example.com:8883",
    enroll_url: str = "https://enroll.example.com",
    expires_at: str = "2030-01-01T00:00:00Z",
    renew_url: str | None = None,
) -> None:
    """Write a complete, `is_enrolled()`-satisfying identity directly to disk.

    Mirrors `test/sink/test_startup_streams.py`'s `_write_identity` helper:
    these tests only need `is_enrolled()`/`load_identity()` to see a
    complete identity, not a real enrollment round trip (that's
    `test_identity.py`'s job).
    """
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "robot.key").write_bytes(b"fake-key-material")
    (directory / "robot.crt").write_bytes(b"fake-cert-material")
    (directory / "ca.crt").write_bytes(b"fake-ca-material")
    doc = {
        "tenant": tenant,
        "robot_id": robot_id,
        "broker_url": broker_url,
        "enroll_url": enroll_url,
        "expires_at": expires_at,
    }
    if renew_url is not None:
        doc["renew_url"] = renew_url
    (directory / "identity.yaml").write_text(yaml.safe_dump(doc))


def _running_service(
    tmp_path: pathlib.Path, publisher: FakePublisher | None = None
) -> tuple[FleetService, FakePublisher]:
    writer = FakeWriter(_imu_struct())
    sink = FakeSink({"/imu": writer})
    pub = publisher if publisher is not None else FakePublisher()
    service = FleetService(
        sink=sink, streams=_imu_streams(), publisher=pub, spool=Spool(tmp_path / "spool")
    )
    service.start()
    return service, pub


@pytest.fixture(autouse=True)
def _isolated_fleet_state(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "FLEET_IDENTITY_DIRECTORY", str(tmp_path / "identity"))
    monkeypatch.setattr(settings, "FLEET_ENABLED", True)
    startup.set_fleet_service(None)
    yield
    service = startup.fleet_service()
    if service is not None:
        service.stop()
    startup.set_fleet_service(None)


class TestFleetStatusNoGate:
    """`fleet_status()` must never raise -- it REPORTS every state instead."""

    def test_not_installed_reports_false_without_raising(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Same idiom the gate tests rely on (test_gate.py): the leaf submodule
        # set to None in sys.modules makes both `importlib.util.find_spec`
        # (control.fleet_status) and a plain `import paho.mqtt.client`
        # (require_fleet, exercised by the pause/resume tests below) behave
        # as "not installed" -- without needing a real uninstall.
        _simulate_paho_not_installed(monkeypatch)
        status = control.fleet_status()
        assert status["installed"] is False

    def test_disabled_reports_false_without_raising(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)
        status = control.fleet_status()
        assert status["enabled"] is False

    def test_unenrolled_reports_no_identity(self) -> None:
        status = control.fleet_status()
        assert status["enrolled"] is False
        assert status["identity"] is None

    def test_no_service_reports_stopped_and_empty(self) -> None:
        status = control.fleet_status()
        assert status["service"] == "stopped"
        assert status["channels"] == []
        assert status["status"] is None

    def test_enrolled_and_running_reports_identity_and_status(self, tmp_path: pathlib.Path) -> None:
        _write_identity(
            pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY),
            expires_at="2030-06-01T00:00:00Z",
            renew_url="https://renew.example.com",
        )
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        status = control.fleet_status()

        assert status["enrolled"] is True
        assert set(status["identity"]) == {
            "tenant",
            "robot_id",
            "broker_url",
            "cert_expires_at",
            "renew_url",
        }
        assert status["identity"]["tenant"] == "acme"
        assert status["identity"]["robot_id"] == "robot-1"
        assert status["identity"]["broker_url"] == "mqtts://fleet.example.com:8883"
        assert status["identity"]["cert_expires_at"] == "2030-06-01T00:00:00Z"
        assert status["identity"]["renew_url"] == "https://renew.example.com"

        assert status["service"] == "running"
        assert status["channels"] == service.channels
        assert "queue" in status["status"]
        assert "spool" in status["status"]

    def test_identity_never_carries_key_material_or_paths(self, tmp_path: pathlib.Path) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        status = control.fleet_status()
        blob = repr(status["identity"])
        assert "key_path" not in blob
        assert "cert_path" not in blob
        assert "ca_path" not in blob
        assert "fake-key-material" not in blob

    def test_paused_service_reports_paused(self, tmp_path: pathlib.Path) -> None:
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)
        service.pause()

        status = control.fleet_status()

        assert status["service"] == "paused"


class TestPauseStreaming:
    def test_running_service_pauses_and_records_reason(self, tmp_path: pathlib.Path) -> None:
        service, pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        result = control.pause_streaming()

        assert result == {"service": "paused", "changed": True, "discarded": False}
        assert pub.close_reasons == ["paused"]

    def test_second_pause_is_a_no_op(self, tmp_path: pathlib.Path) -> None:
        service, pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        control.pause_streaming()
        result = control.pause_streaming()

        assert result == {"service": "paused", "changed": False, "discarded": False}
        assert pub.close_reasons == ["paused"]  # no second close

    def test_discard_true_passes_through_and_empties_channels_lane(
        self, tmp_path: pathlib.Path
    ) -> None:
        service, _pub = _running_service(tmp_path)
        spool = service._spool
        seq = spool.next_seq("channels")
        spool.append("channels", seq, {"v": 1, "samples": []})
        assert list(spool.pending("channels"))
        startup.set_fleet_service(service)

        result = control.pause_streaming(discard=True)

        assert result == {"service": "paused", "changed": True, "discarded": True}
        assert list(spool.pending("channels")) == []

    def test_no_service_is_idempotent_no_op(self) -> None:
        assert control.pause_streaming() == {"service": "stopped", "changed": False}

    def test_disabled_raises_before_holder_access(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service, _pub = _running_service(tmp_path)

        def poison(*_a: object, **_kw: object) -> None:
            raise AssertionError("pause() must not be reached: FLEET_ENABLED=0 gates first")

        service.pause = poison  # type: ignore[method-assign]
        startup.set_fleet_service(service)
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)

        with pytest.raises(FleetDisabledError, match="FLEET_ENABLED"):
            control.pause_streaming()

    def test_not_installed_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)
        _simulate_paho_not_installed(monkeypatch)

        with pytest.raises(FleetNotInstalledError):
            control.pause_streaming()


class TestResumeStreaming:
    def test_paused_service_resumes(self, tmp_path: pathlib.Path) -> None:
        service, pub = _running_service(tmp_path)
        service.pause()
        startup.set_fleet_service(service)

        result = control.resume_streaming()

        assert result == {"service": "running", "changed": True}
        assert pub.close_reasons == ["paused"]  # resume doesn't close

    def test_already_running_service_is_a_no_op(self, tmp_path: pathlib.Path) -> None:
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        result = control.resume_streaming()

        assert result == {"service": "running", "changed": False}

    def test_second_resume_is_a_no_op(self, tmp_path: pathlib.Path) -> None:
        service, _pub = _running_service(tmp_path)
        service.pause()
        startup.set_fleet_service(service)

        control.resume_streaming()
        result = control.resume_streaming()

        assert result == {"service": "running", "changed": False}

    def test_no_service_is_idempotent_no_op(self) -> None:
        assert control.resume_streaming() == {"service": "stopped", "changed": False}

    def test_disabled_raises_before_holder_access(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service, _pub = _running_service(tmp_path)
        service.pause()

        def poison(*_a: object, **_kw: object) -> None:
            raise AssertionError("resume() must not be reached: FLEET_ENABLED=0 gates first")

        service.resume = poison  # type: ignore[method-assign]
        startup.set_fleet_service(service)
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)

        with pytest.raises(FleetDisabledError, match="FLEET_ENABLED"):
            control.resume_streaming()

    def test_not_installed_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service, _pub = _running_service(tmp_path)
        service.pause()
        startup.set_fleet_service(service)
        _simulate_paho_not_installed(monkeypatch)

        with pytest.raises(FleetNotInstalledError):
            control.resume_streaming()
