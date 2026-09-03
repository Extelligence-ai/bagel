"""control.py: enroll/unenroll/status/pause/resume/stream-rule control (fleet streaming step 7).

`fleet_status()` calls NO gate -- it is the local source of truth in every
state (uninstalled, disabled, unenrolled, service down); `unenroll_identity()`
is the other gate-free operation (it only makes the subsystem MORE inert).
Every other operation here calls `require_fleet()` first, so `FLEET_ENABLED=0`
raises before the holder is ever touched.
"""

import importlib
import os
import pathlib
import stat
import sys
import threading

import pyarrow as pa
import pytest
import yaml

from publish.conftest import FakePublisher, FakeSink, FakeWriter, _imu_streams, _imu_struct
from publish.test_identity import VALID_TOKEN, fake_server
from settings import settings
from src.sink import base as sink_base
from src.sink import startup
from src.sink.publish import (
    EnrollmentError,
    FleetDisabledError,
    FleetNotEnrolledError,
    FleetNotInstalledError,
    StreamConfigError,
    config,
    control,
    identity,
)
from src.sink.publish.service import FleetService
from src.sink.publish.spool import Spool, SpoolCorruptError

# `fake_server` is imported only to be re-exported as a pytest fixture (used
# by parameter name in TestEnrollIdentity, never referenced by name in this
# module's own code) -- see test_service.py's identical idiom.
__all__ = ["fake_server"]


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
    monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", None)
    startup.set_fleet_service(None)
    yield
    service = startup.fleet_service()
    if service is not None:
        service.stop()
    startup.set_fleet_service(None)


@pytest.fixture(autouse=True)
def _fake_mqtt_publisher(monkeypatch: pytest.MonkeyPatch) -> list[FakePublisher]:
    """`control._restart_service` builds a real `MqttPublisher` -- swap it for a
    `FakePublisher` so `stream_topics`/`stop_streams` tests never touch a real
    socket. Harmless (never invoked) for tests that don't trigger a restart.
    """
    built: list[FakePublisher] = []

    def factory(**_kwargs: object) -> FakePublisher:
        pub = FakePublisher()
        built.append(pub)
        return pub

    monkeypatch.setattr(control, "MqttPublisher", factory)
    return built


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
        assert status["events"] == []
        assert status["status"] is None

    def test_running_service_with_event_rules_lists_their_names(
        self, tmp_path: pathlib.Path
    ) -> None:
        streams = config.StreamsConfig.build(
            {
                "channels": [{"topic": "/imu", "fields": ["x"], "rate_hz": 50}],
                "events": [{"name": "hard_decel", "topic": "/imu", "predicate": "true"}],
            }
        )
        struct = pa.struct([pa.field("x", pa.float64())])
        sink = FakeSink({"/imu": FakeWriter(struct)})
        service = FleetService(
            sink=sink, streams=streams, publisher=FakePublisher(), spool=Spool(tmp_path / "spool")
        )
        service.start()
        startup.set_fleet_service(service)

        status = control.fleet_status()

        assert status["events"] == ["hard_decel"]

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

    def test_load_identity_raising_between_checks_degrades_gracefully(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TOCTOU regression: `fleet_status()` must single-load `identity.load_identity`,
        not check `is_enrolled()` and then separately `load_identity()`.

        A stale `is_enrolled() -> True` immediately followed by a
        `load_identity()` that raises (the identity was deleted/corrupted in
        between) must never surface as an unhandled `FleetNotEnrolledError` --
        `fleet_status()` never raises. Proven structurally: `is_enrolled` is
        made to return a stale `True` while `load_identity` always raises --
        a two-call implementation would propagate that raise; a single-load
        try/except cannot, since it never consults `is_enrolled` at all.
        """
        monkeypatch.setattr(identity, "is_enrolled", lambda *_a, **_kw: True)

        def _raise(*_a: object, **_kw: object) -> None:
            raise FleetNotEnrolledError("deleted between checks")

        monkeypatch.setattr(identity, "load_identity", _raise)

        status = control.fleet_status()

        assert status["enrolled"] is False
        assert status["identity"] is None

    def test_find_spec_raising_module_not_found_reports_not_installed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """P2 (Codex round 3): `find_spec("paho.mqtt")` itself can raise when the
        parent `paho` package on the path is a half-installed namespace package
        -- must degrade to "not installed", never propagate out of a
        never-raises function."""

        def _boom(_name: str) -> None:
            raise ModuleNotFoundError("paho")

        monkeypatch.setattr(control.importlib.util, "find_spec", _boom)

        status = control.fleet_status()

        assert status["installed"] is False

    def test_find_spec_raising_value_error_reports_not_installed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same guard, the other exception `find_spec` can raise for a broken
        namespace package."""

        def _boom(_name: str) -> None:
            raise ValueError("half-installed namespace package")

        monkeypatch.setattr(control.importlib.util, "find_spec", _boom)

        status = control.fleet_status()

        assert status["installed"] is False

    def test_corrupt_spool_status_reports_error_instead_of_raising(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """P2 (Codex round 3): `FleetService.status()` rescans spool segments on
        first access and can raise `SpoolCorruptError` -- `fleet_status()` must
        surface that as `status: {"error": ...}`, never let it propagate."""
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        def _boom() -> dict:
            raise SpoolCorruptError("lane 'channels': segment corrupt")

        monkeypatch.setattr(service, "status", _boom)

        status = control.fleet_status()

        assert status["status"] == {"error": "SpoolCorruptError: lane 'channels': segment corrupt"}


def test_control_module_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lazy-import regression for control.py itself -- mirrors test_gate.py's/
    test_service.py's idiom. control.py imports `connect`/`config`/`spool`/`mqtt`
    (and `service`) at module scope; none of those may drag paho or cryptography
    in eagerly either.
    """
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.control", raising=False)
    importlib.import_module("src.sink.publish.control")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )


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

    def test_discard_true_while_already_paused_still_empties_and_reports_unchanged(
        self, tmp_path: pathlib.Path
    ) -> None:
        """P2 (Codex round 3): `changed: False` (the service was already
        paused) but `discarded: True` still actually empties the backlog --
        the tool-level ruling `{"service": "paused", "changed": False,
        "discarded": True}`."""
        service, _pub = _running_service(tmp_path)
        spool = service._spool
        startup.set_fleet_service(service)
        control.pause_streaming()  # first pause, no discard

        seq = spool.next_seq("channels")
        spool.append("channels", seq, {"v": 1, "samples": []})
        assert list(spool.pending("channels"))

        result = control.pause_streaming(discard=True)

        assert result == {"service": "paused", "changed": False, "discarded": True}
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


def _assert_no_token_anywhere(obj: object, token: str) -> None:
    """Recursively assert `token` never appears -- as a dict key or a substring
    of any string value -- anywhere in `obj`."""
    if isinstance(obj, dict):
        assert "token" not in obj
        for value in obj.values():
            _assert_no_token_anywhere(value, token)
    elif isinstance(obj, list):
        for value in obj:
            _assert_no_token_anywhere(value, token)
    elif isinstance(obj, str):
        assert token not in obj


class TestEnrollIdentity:
    def test_happy_path_writes_identity_under_tmp_directory(self, fake_server: str) -> None:
        result = control.enroll_identity(VALID_TOKEN, fake_server)

        directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
        assert (directory / "robot.key").is_file()
        assert (directory / "robot.crt").is_file()
        assert (directory / "ca.crt").is_file()
        assert (directory / "identity.yaml").is_file()
        assert result == {
            "tenant": "acme",
            "robot_id": "robot-42",
            "broker_url": "mqtts://fleet.example.com:8883",
            "expires_at": "2027-01-01T00:00:00Z",
        }

    def test_already_enrolled_raises_with_the_ruling_message(self) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

        with pytest.raises(EnrollmentError, match="already enrolled") as exc_info:
            control.enroll_identity("some-token", "https://enroll.example.com")

        assert exc_info.value.status == 0

    def test_result_dict_never_carries_the_token(self, fake_server: str) -> None:
        result = control.enroll_identity(VALID_TOKEN, fake_server)
        _assert_no_token_anywhere(result, VALID_TOKEN)

    def test_disabled_raises_before_any_keygen(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def poison(*_a: object, **_kw: object) -> None:
            raise AssertionError(
                "generate_key_and_csr must not be reached: FLEET_ENABLED=0 gates first"
            )

        monkeypatch.setattr(identity, "generate_key_and_csr", poison)
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)

        with pytest.raises(FleetDisabledError, match="FLEET_ENABLED"):
            control.enroll_identity("some-token", "https://enroll.example.com")

    def test_not_installed_raises_before_any_keygen(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """M1: paho not installed raises via require_fleet(), before any keygen."""

        def poison(*_a: object, **_kw: object) -> None:
            raise AssertionError(
                "generate_key_and_csr must not be reached: paho not installed gates first"
            )

        monkeypatch.setattr(identity, "generate_key_and_csr", poison)
        _simulate_paho_not_installed(monkeypatch)

        with pytest.raises(FleetNotInstalledError):
            control.enroll_identity("some-token", "https://enroll.example.com")

    def test_enroll_while_dev_service_running_leaves_it_unchanged(
        self, tmp_path: pathlib.Path, fake_server: str
    ) -> None:
        """M2: enrolling while a dev-identity service is already running (an
        unenrolled robot's placeholder `dev/robot` service) must not
        retroactively touch it -- the new identity only takes effect on the
        next restart/rule change (stream_topics/stop_streams)."""
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)
        assert service.status()["cert_expires_at"] is None

        result = control.enroll_identity(VALID_TOKEN, fake_server)

        assert result["robot_id"] == "robot-42"
        assert startup.fleet_service() is service  # unchanged, same object
        assert service.status()["cert_expires_at"] is None  # still the dev-identity service


class TestUnenrollIdentity:
    def test_running_service_stopped_and_holder_cleared(self, tmp_path: pathlib.Path) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        service, pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        result = control.unenroll_identity()

        assert pub.close_calls >= 1
        assert startup.fleet_service() is None
        assert result["service"] == "stopped"

    def test_delete_identity_is_the_only_deletion_path(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Replace `identity.delete_identity` with a no-op fake that never touches
        disk: if `unenroll_identity()` did its own separate unlinking, the files
        would still be gone; since it must go ONLY through `delete_identity`,
        they must survive untouched."""
        directory = pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY)
        _write_identity(directory)
        calls: list[pathlib.Path] = []

        def fake_delete(dir_arg: pathlib.Path) -> list[str]:
            calls.append(pathlib.Path(dir_arg))
            return ["identity.yaml"]

        monkeypatch.setattr(identity, "delete_identity", fake_delete)

        result = control.unenroll_identity()

        assert calls == [directory]
        assert result["deleted"] == ["identity.yaml"]
        assert (directory / "identity.yaml").is_file()  # untouched by unenroll_identity itself
        assert (directory / "robot.key").is_file()

    def test_manifest_streams_section_removed_subscriptions_preserved(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        manifest_path = tmp_path / "startup.yaml"
        subscriptions = [{"sink": "mqtt", "topics": ["/imu"]}]
        manifest_path.write_text(
            yaml.safe_dump(
                {
                    "subscriptions": subscriptions,
                    "streams": {"channels": [{"topic": "/imu", "fields": ["x"], "rate_hz": 5}]},
                },
                sort_keys=False,
            )
        )
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))

        result = control.unenroll_identity()

        assert result["streams_removed"] is True
        doc = yaml.safe_load(manifest_path.read_text())
        assert "streams" not in doc
        assert doc["subscriptions"] == subscriptions

    def test_second_call_is_idempotent_no_error(self) -> None:
        assert control.unenroll_identity()["deleted"] == []
        assert control.unenroll_identity()["deleted"] == []

    def test_works_with_fleet_disabled(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)

        result = control.unenroll_identity()  # must not raise

        assert result["deleted"]
        assert control.fleet_status()["enrolled"] is False


class TestPersistStreamsManifestHandling:
    """`_persist_streams`/`_read_manifest_doc`: the shared manifest read/write path."""

    def test_unset_manifest_file_returns_false_without_error(self) -> None:
        assert control._persist_streams({"channels": [], "events": []}) is False

    def test_unparsable_existing_manifest_raises_without_clobbering(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manifest_path = tmp_path / "manifest.yaml"
        original_text = "not: valid: yaml: ["
        manifest_path.write_text(original_text)
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))

        with pytest.raises(StreamConfigError, match="manifest"):
            control._persist_streams({"channels": [], "events": []})

        assert manifest_path.read_text() == original_text

    def test_missing_manifest_file_persists_from_empty(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manifest_path = tmp_path / "manifest.yaml"
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))

        result = control._persist_streams({"channels": [], "events": []})

        assert result is True
        doc = yaml.safe_load(manifest_path.read_text())
        assert doc == {"streams": {"channels": [], "events": []}}

    def test_preserves_the_manifest_files_existing_permission_mode(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`tempfile.mkstemp` creates at 0600 by default -- without copying the
        pre-existing file's mode onto the tempfile before `os.replace`, a
        manifest a human left more permissive (or read-only) would silently
        end up 0600 after its first control-plane write."""
        manifest_path = tmp_path / "manifest.yaml"
        manifest_path.write_text(yaml.safe_dump({"subscriptions": []}))
        manifest_path.chmod(0o644)
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))

        control._persist_streams({"channels": [], "events": []})

        assert stat.S_IMODE(manifest_path.stat().st_mode) == 0o644


class TestStreamTopics:
    """`stream_topics` validates, merges onto the current config, restarts, persists."""

    @pytest.fixture(autouse=True)
    def _enrolled(self) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

    def _running_multi_topic_service(self, tmp_path: pathlib.Path) -> FleetService:
        writer_imu = FakeWriter(_imu_struct())
        writer_odom = FakeWriter(pa.struct([pa.field("y", pa.float64())]))
        sink = FakeSink({"/imu": writer_imu, "/odom": writer_odom})
        service = FleetService(
            sink=sink,
            streams=_imu_streams(),
            publisher=FakePublisher(),
            spool=Spool(tmp_path / "spool"),
        )
        service.start()
        startup.set_fleet_service(service)
        return service

    def test_running_service_with_new_channel_restarts_and_merges(
        self, tmp_path: pathlib.Path
    ) -> None:
        old_service = self._running_multi_topic_service(tmp_path)

        result = control.stream_topics(
            channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
        )

        new_service = startup.fleet_service()
        assert new_service is not None
        assert new_service is not old_service
        names = {c["c"] for c in new_service.channels}
        assert {"imu.x", "odom.y"} <= names
        assert result["service"] == "running"
        assert result["events"] == []

    def test_unparsable_manifest_does_not_undo_a_successful_restart(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """P2 (Codex round 3): live-vs-persisted atomicity -- a valid rule
        change against a RUNNING service (so `_current_streams()` never
        touches the manifest) must still restart the service even when the
        configured manifest file is unparsable; the persist failure is
        reported, not raised, and the bad file is left untouched."""
        old_service = self._running_multi_topic_service(tmp_path)
        manifest_path = tmp_path / "manifest.yaml"
        original_text = "not: valid: yaml: ["
        manifest_path.write_text(original_text)
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))

        result = control.stream_topics(
            channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
        )

        new_service = startup.fleet_service()
        assert new_service is not None
        assert new_service is not old_service
        names = {c["c"] for c in new_service.channels}
        assert {"imu.x", "odom.y"} <= names
        assert result["persisted"] is False
        assert "persist_error" in result
        assert "unparsable manifest" in result["persist_error"]
        assert manifest_path.read_text() == original_text

    @pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="root bypasses directory permission checks",
    )
    def test_read_only_manifest_directory_does_not_undo_a_successful_restart(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """P2 follow-up (Codex round 3, PR #214): `_persist_or_report` widened
        to also catch `OSError` -- a read-only manifest directory (disk full,
        permission denied, etc) must not undo an already-successful restart
        either, same as the unparsable-manifest StreamConfigError case."""
        old_service = self._running_multi_topic_service(tmp_path)
        manifest_dir = tmp_path / "readonly"
        manifest_dir.mkdir(mode=0o555)
        manifest_path = manifest_dir / "manifest.yaml"
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))

        try:
            result = control.stream_topics(
                channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
            )

            new_service = startup.fleet_service()
            assert new_service is not None
            assert new_service is not old_service
            names = {c["c"] for c in new_service.channels}
            assert {"imu.x", "odom.y"} <= names
            assert result["persisted"] is False
            assert "persist_error" in result
            assert not manifest_path.exists()
        finally:
            manifest_dir.chmod(0o755)  # restore write access so tmp_path cleanup can proceed

    def test_same_topic_rule_is_replaced_not_duplicated(self, tmp_path: pathlib.Path) -> None:
        self._running_multi_topic_service(tmp_path)

        control.stream_topics(
            channels=[{"topic": "/imu", "fields": ["x"], "rate_hz": 10}], events=None
        )

        new_service = startup.fleet_service()
        assert len(new_service.streams.channels) == 1
        assert new_service.streams.channels[0].rate_hz == 10.0

    def test_manifest_persisted_and_reloadable(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manifest_path = tmp_path / "manifest.yaml"
        manifest_path.write_text(yaml.safe_dump({"subscriptions": []}))
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))
        self._running_multi_topic_service(tmp_path)

        result = control.stream_topics(
            channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
        )

        assert result["persisted"] is True
        doc = yaml.safe_load(manifest_path.read_text())
        reloaded = config.load_streams(doc)
        assert reloaded is not None
        topics = {rule.topic for rule in reloaded.channels}
        assert {"/imu", "/odom"} <= topics

    def test_no_service_but_a_covering_live_sink_starts_one(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        monkeypatch.setattr(sink_base, "live_sinks", lambda: [sink])
        startup.set_fleet_service(None)

        result = control.stream_topics(
            channels=[{"topic": "/imu", "fields": ["x"], "rate_hz": 5}], events=None
        )

        assert result["service"] == "running"
        assert startup.fleet_service() is not None

    def test_no_covering_sink_raises_and_leaves_holder_and_manifest_untouched(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_base, "live_sinks", lambda: [])
        manifest_path = tmp_path / "manifest.yaml"
        original = {"subscriptions": [{"sink": "mqtt"}]}
        manifest_path.write_text(yaml.safe_dump(original))
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))
        startup.set_fleet_service(None)

        with pytest.raises(StreamConfigError):
            control.stream_topics(
                channels=[{"topic": "/imu", "fields": ["x"], "rate_hz": 5}], events=None
            )

        assert startup.fleet_service() is None
        assert yaml.safe_load(manifest_path.read_text()) == original

    def test_invalid_rule_dict_raises_before_any_restart(self, tmp_path: pathlib.Path) -> None:
        service = self._running_multi_topic_service(tmp_path)

        with pytest.raises(StreamConfigError):
            control.stream_topics(channels=[{"topic": "/imu", "rate_hz": 5}], events=None)

        assert startup.fleet_service() is service

    def test_no_manifest_file_configured_persisted_false_but_rules_live(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", None)
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        monkeypatch.setattr(sink_base, "live_sinks", lambda: [sink])

        result = control.stream_topics(
            channels=[{"topic": "/imu", "fields": ["x"], "rate_hz": 5}], events=None
        )

        assert result["persisted"] is False
        assert startup.fleet_service() is not None

    def test_uncovered_topic_raises_and_leaves_the_original_service_running(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Codex review (F1): the coverage check must run BEFORE the old service
        is stopped -- a typo'd/uncovered topic must not destroy a working
        service. The OLD service object stays in the holder (identity check,
        not just "a" service) and is still fully functional afterward."""
        old_service = self._running_multi_topic_service(tmp_path)

        with pytest.raises(StreamConfigError):
            control.stream_topics(
                channels=[{"topic": "/typo/topic", "fields": ["x"], "rate_hz": 5}], events=None
            )

        assert startup.fleet_service() is old_service
        assert startup.fleet_service().status() is not None

    def test_load_identity_or_none_is_a_single_load_not_check_then_load(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review (F2): `_restart_service` must not reintroduce the
        `is_enrolled()` + separate `load_identity()` TOCTOU that Task 4's
        carried finding removed from `fleet_status()`. `is_enrolled` lying
        `True` while `load_identity` always raises proves this helper never
        consults `is_enrolled` at all -- a two-call implementation would
        propagate the raise."""
        monkeypatch.setattr(identity, "is_enrolled", lambda *_a, **_kw: True)

        def _raise(*_a: object, **_kw: object) -> None:
            raise FleetNotEnrolledError("deleted between checks")

        monkeypatch.setattr(identity, "load_identity", _raise)

        assert control._load_identity_or_none() is None

    def test_identity_resolution_failure_never_stops_the_old_service_first(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review (F2): identity resolution must happen BEFORE the old
        service is stopped, combined with F1's pre-checks. Forcing
        `load_identity` to degrade to `None` (a deleted/corrupt identity)
        means `resolve_publisher_kwargs` itself then raises
        `FleetNotEnrolledError` (no broker configured, no identity) -- and
        that must still leave the OLD service running, untouched."""
        old_service = self._running_multi_topic_service(tmp_path)

        def _raise(*_a: object, **_kw: object) -> None:
            raise FleetNotEnrolledError("deleted between checks")

        monkeypatch.setattr(identity, "load_identity", _raise)

        with pytest.raises(FleetNotEnrolledError):
            control.stream_topics(
                channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
            )

        assert startup.fleet_service() is old_service
        assert startup.fleet_service().status() is not None

    def test_event_rule_merge_by_name_through_stream_topics(self, tmp_path: pathlib.Path) -> None:
        """Integration-level (F5c): the merge-by-name ruling for events, exercised
        through the public `stream_topics` entry point rather than the
        `_merge_by_key` unit alone."""
        self._running_multi_topic_service(tmp_path)

        # Predicates must be valid against the topic's struct since Task 7:
        # FleetService.start() probes them via events.validate_predicates.
        control.stream_topics(
            channels=None,
            events=[{"name": "hard_decel", "topic": "/imu", "predicate": "\"/imu\"['x'] < -10"}],
        )
        result = control.stream_topics(
            channels=None,
            events=[
                {
                    "name": "hard_decel",
                    "topic": "/imu",
                    "predicate": "\"/imu\"['x'] < -20",
                    "pre_seconds": 5,
                }
            ],
        )

        new_service = startup.fleet_service()
        assert len(new_service.streams.events) == 1
        assert new_service.streams.events[0].predicate == "\"/imu\"['x'] < -20"
        assert new_service.streams.events[0].pre_seconds == 5.0
        assert result["events"] == ["hard_decel"]

    def test_bad_predicate_raises_and_leaves_the_original_service_running(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Rider (a): a bad event predicate is now caught at service-(re)start
        time, via `_restart_service`'s new `events.validate_predicates` call
        -- BEFORE the old service is stopped, per the existing
        failure-outcome contract. The OLD service object (identity check)
        must still be in the holder, and still fully functional."""
        old_service = self._running_multi_topic_service(tmp_path)

        with pytest.raises(StreamConfigError):
            control.stream_topics(
                channels=None,
                events=[
                    {"name": "bad", "topic": "/imu", "predicate": "not valid sql (("}
                ],
            )

        assert startup.fleet_service() is old_service
        assert startup.fleet_service().status() is not None

    def test_stale_persisted_bad_predicate_rule_is_also_caught(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Same protection when the bad rule is carried over unchanged from
        the current config (e.g. a stale manifest-persisted rule with a
        bare-column predicate) rather than freshly submitted -- merging in
        an unrelated channel rule must still trip the predicate pre-check on
        the carried-over event rule."""
        self._running_multi_topic_service(tmp_path)
        control.stream_topics(
            channels=None,
            events=[{"name": "hard_decel", "topic": "/imu", "predicate": "\"/imu\"['x'] < -10"}],
        )
        stale_service = startup.fleet_service()
        assert stale_service is not None
        # Corrupt the already-merged rule in place to simulate a stale
        # persisted rule with a bare-column (unquoted-topic) predicate that
        # would raise inside evaluate_predicate.
        stale_service.streams.events[0].predicate = "x < -10"

        with pytest.raises(StreamConfigError):
            control.stream_topics(
                channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
            )

        assert startup.fleet_service() is stale_service
        assert startup.fleet_service().status() is not None

    def test_event_topic_not_covered_by_sink_raises_and_leaves_service_running(
        self, tmp_path: pathlib.Path
    ) -> None:
        """The coverage pre-check (`_resolve_sink`) already protects an event
        topic the sink doesn't subscribe to -- verified explicitly here per
        rider (a)'s instruction, not just relying on the channels-topic
        coverage test."""
        old_service = self._running_multi_topic_service(tmp_path)

        with pytest.raises(StreamConfigError):
            control.stream_topics(
                channels=None,
                events=[
                    {"name": "bad", "topic": "/not/subscribed", "predicate": "true"}
                ],
            )

        assert startup.fleet_service() is old_service
        assert startup.fleet_service().status() is not None

    def test_no_event_rules_configured(self, tmp_path: pathlib.Path) -> None:
        """`events` is empty when no event rules are configured."""
        self._running_multi_topic_service(tmp_path)

        result = control.stream_topics(
            channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
        )

        assert result["events"] == []

    def test_disabled_raises_before_any_state_change(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """M1: FLEET_ENABLED=0 raises before touching the holder/validation at all."""
        old_service = self._running_multi_topic_service(tmp_path)
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)

        with pytest.raises(FleetDisabledError, match="FLEET_ENABLED"):
            control.stream_topics(
                channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
            )

        assert startup.fleet_service() is old_service

    def test_not_installed_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """M1: paho not installed raises via require_fleet(), before any state change."""
        old_service = self._running_multi_topic_service(tmp_path)
        _simulate_paho_not_installed(monkeypatch)

        with pytest.raises(FleetNotInstalledError):
            control.stream_topics(
                channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
            )

        assert startup.fleet_service() is old_service

    def test_paused_service_stays_paused_across_a_rule_change(self, tmp_path: pathlib.Path) -> None:
        """I1 (important): a rule change must not silently un-pause a paused
        service -- the brief reconnect-to-republish-schema blip is accepted,
        but the service must land back in `paused`, not `running`."""
        old_service = self._running_multi_topic_service(tmp_path)
        old_service.pause()
        assert old_service.paused is True

        result = control.stream_topics(
            channels=[{"topic": "/odom", "fields": ["y"], "rate_hz": 2}], events=None
        )

        new_service = startup.fleet_service()
        assert new_service is not old_service
        assert new_service.paused is True
        assert result["service"] == "paused"

        resume_result = control.resume_streaming()
        assert resume_result == {"service": "running", "changed": True}
        assert new_service.paused is False


class TestStopStreams:
    """`stop_streams` removes rules by resolved name; unknown names are idempotent no-ops."""

    @pytest.fixture(autouse=True)
    def _enrolled(self) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

    def _service_with_streams(
        self, tmp_path: pathlib.Path, streams: object, topic: str = "/imu"
    ) -> FleetService:
        struct = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.float64())])
        sink = FakeSink({topic: FakeWriter(struct)})
        service = FleetService(
            sink=sink, streams=streams, publisher=FakePublisher(), spool=Spool(tmp_path / "spool")
        )
        service.start()
        startup.set_fleet_service(service)
        return service

    def test_removes_renamed_channel_by_its_renamed_name(self, tmp_path: pathlib.Path) -> None:
        streams = config.StreamsConfig.build(
            {
                "channels": [
                    {"topic": "/imu", "fields": ["x"], "rate_hz": 50, "as": {"x": "accel.x"}}
                ]
            }
        )
        self._service_with_streams(tmp_path, streams)

        result = control.stop_streams(channels=["accel.x"], events=None)

        new_service = startup.fleet_service()
        assert new_service.streams.channels == []
        assert result["changed"] is True

    def test_partial_field_removal_keeps_rule_with_remaining_field(
        self, tmp_path: pathlib.Path
    ) -> None:
        streams = config.StreamsConfig.build(
            {"channels": [{"topic": "/imu", "fields": ["x", "y"], "rate_hz": 50}]}
        )
        self._service_with_streams(tmp_path, streams)

        result = control.stop_streams(channels=["imu.x"], events=None)

        new_service = startup.fleet_service()
        assert len(new_service.streams.channels) == 1
        assert new_service.streams.channels[0].fields == ["y"]
        assert result["changed"] is True

    def test_rule_dropped_when_all_its_fields_go(self, tmp_path: pathlib.Path) -> None:
        streams = config.StreamsConfig.build(
            {"channels": [{"topic": "/imu", "fields": ["x", "y"], "rate_hz": 50}]}
        )
        self._service_with_streams(tmp_path, streams)

        control.stop_streams(channels=["imu.x", "imu.y"], events=None)

        assert startup.fleet_service().streams.channels == []

    def test_geo_rule_removed_by_its_name(self, tmp_path: pathlib.Path) -> None:
        streams = config.StreamsConfig.build(
            {"channels": [{"topic": "/nav/odom", "geo": {"lat": "x", "lon": "y"}, "rate_hz": 1}]}
        )
        self._service_with_streams(tmp_path, streams, topic="/nav/odom")

        result = control.stop_streams(channels=["odom.geo"], events=None)

        assert startup.fleet_service().streams.channels == []
        assert result["changed"] is True

    def test_event_removed_by_name(self, tmp_path: pathlib.Path) -> None:
        streams = config.StreamsConfig.build(
            {
                "channels": [],
                "events": [{"name": "hard_decel", "topic": "/imu", "predicate": "true"}],
            }
        )
        self._service_with_streams(tmp_path, streams)

        result = control.stop_streams(channels=None, events=["hard_decel"])

        assert startup.fleet_service().streams.events == []
        assert result["changed"] is True
        assert result["events"] == []

    def test_leftover_event_rule_after_stop_streams_reports_it_still_live(
        self, tmp_path: pathlib.Path
    ) -> None:
        """An event rule left in place after a channel removal still shows up
        in `events` -- it's live, not merely stored."""
        streams = config.StreamsConfig.build(
            {
                "channels": [{"topic": "/imu", "fields": ["x"], "rate_hz": 50}],
                "events": [{"name": "hard_decel", "topic": "/imu", "predicate": "true"}],
            }
        )
        self._service_with_streams(tmp_path, streams)

        result = control.stop_streams(channels=["imu.x"], events=None)

        assert result["events"] == ["hard_decel"]

    def test_unknown_name_is_a_no_op_and_does_not_restart(self, tmp_path: pathlib.Path) -> None:
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        result = control.stop_streams(channels=["nope.x"], events=["nope"])

        assert result["changed"] is False
        assert startup.fleet_service() is service

    def test_unknown_name_no_op_does_not_touch_a_manifest_with_no_streams_section(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review (F3): a pure no-op must skip `_persist_streams` entirely --
        not just skip the restart -- so a manifest with no `streams:` section
        stays byte-identical and `persisted` truthfully reports `False`."""
        manifest_path = tmp_path / "manifest.yaml"
        original_text = yaml.safe_dump({"subscriptions": []})
        manifest_path.write_text(original_text)
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        result = control.stop_streams(channels=["nope.x"], events=["nope"])

        assert result["changed"] is False
        assert result["persisted"] is False
        assert manifest_path.read_text() == original_text

    def test_emptying_config_keeps_service_running_with_alive_heartbeat(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        manifest_path = tmp_path / "manifest.yaml"
        manifest_path.write_text(yaml.safe_dump({"subscriptions": []}))
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))
        service, _pub = _running_service(tmp_path)
        startup.set_fleet_service(service)

        result = control.stop_streams(channels=["imu.x"], events=None)

        new_service = startup.fleet_service()
        assert new_service is not None
        assert new_service.status()["heartbeat_alive"] is True
        assert result["service"] == "running"
        doc = yaml.safe_load(manifest_path.read_text())
        assert doc["streams"]["channels"] == []
        assert doc["streams"]["events"] == []
        assert config.load_streams(doc) is not None

    def test_paused_service_stays_paused_across_a_rule_removal(
        self, tmp_path: pathlib.Path
    ) -> None:
        """I1 (important): same ruling as stream_topics's -- a paused service
        must still be paused after stop_streams restarts it."""
        streams = config.StreamsConfig.build(
            {"channels": [{"topic": "/imu", "fields": ["x", "y"], "rate_hz": 50}]}
        )
        old_service = self._service_with_streams(tmp_path, streams)
        old_service.pause()
        assert old_service.paused is True

        result = control.stop_streams(channels=["imu.x"], events=None)

        new_service = startup.fleet_service()
        assert new_service is not old_service
        assert new_service.paused is True
        assert result["service"] == "paused"

        resume_result = control.resume_streaming()
        assert resume_result == {"service": "running", "changed": True}
        assert new_service.paused is False

    def test_unparsable_manifest_does_not_undo_a_successful_restart(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """P2 (Codex round 3): same live-vs-persisted atomicity ruling as
        stream_topics's -- a rule removal that actually changes and restarts
        a running service must not be undone by an unparsable manifest;
        the persist failure is reported, not raised."""
        streams = config.StreamsConfig.build(
            {"channels": [{"topic": "/imu", "fields": ["x", "y"], "rate_hz": 50}]}
        )
        old_service = self._service_with_streams(tmp_path, streams)
        manifest_path = tmp_path / "manifest.yaml"
        original_text = "not: valid: yaml: ["
        manifest_path.write_text(original_text)
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))

        result = control.stop_streams(channels=["imu.x"], events=None)

        new_service = startup.fleet_service()
        assert new_service is not old_service
        assert {c["c"] for c in new_service.channels} == {"imu.y"}
        assert result["changed"] is True
        assert result["persisted"] is False
        assert "persist_error" in result
        assert "unparsable manifest" in result["persist_error"]
        assert manifest_path.read_text() == original_text

    def test_no_service_persist_only_reports_stopped(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """M1: with no service running, stop_streams only updates the
        persisted manifest -- it never starts one -- and reports "stopped"."""
        manifest_path = tmp_path / "manifest.yaml"
        manifest_path.write_text(
            yaml.safe_dump(
                {
                    "subscriptions": [],
                    "streams": {
                        "channels": [{"topic": "/imu", "fields": ["x"], "rate_hz": 50}],
                        "events": [],
                    },
                }
            )
        )
        monkeypatch.setattr(settings, "STARTUP_PIPELINES_FILE", str(manifest_path))
        startup.set_fleet_service(None)

        result = control.stop_streams(channels=["imu.x"], events=None)

        assert result == {
            "service": "stopped",
            "channels": [],
            "events": [],
            "changed": True,
            "persisted": True,
        }
        assert startup.fleet_service() is None
        doc = yaml.safe_load(manifest_path.read_text())
        assert doc["streams"]["channels"] == []


class TestControlLock:
    """I2 (important): one module-level lock serializes tool-driven lifecycle
    transitions across the mutating control-plane operations. `fleet_status`
    stays deliberately unlocked (read-only, must never block) -- not tested
    here since it never touches `_control_lock` at all.
    """

    @pytest.fixture(autouse=True)
    def _enrolled(self) -> None:
        _write_identity(pathlib.Path(settings.FLEET_IDENTITY_DIRECTORY))

    def test_second_mutating_call_blocks_until_the_first_releases_the_lock(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        writer = FakeWriter(_imu_struct())
        sink = FakeSink({"/imu": writer})
        monkeypatch.setattr(sink_base, "live_sinks", lambda: [sink])
        startup.set_fleet_service(None)

        original_restart = control._restart_service
        entered = threading.Event()
        release = threading.Event()

        def blocking_restart(streams: object) -> None:
            entered.set()
            assert release.wait(timeout=5), "test setup: release was never signaled"
            original_restart(streams)

        monkeypatch.setattr(control, "_restart_service", blocking_restart)

        first_done = threading.Event()

        def call_stream_topics() -> None:
            control.stream_topics(
                channels=[{"topic": "/imu", "fields": ["x"], "rate_hz": 5}], events=None
            )
            first_done.set()

        first = threading.Thread(target=call_stream_topics)
        first.start()
        assert entered.wait(timeout=2), "first call never reached the blocked _restart_service"

        # A second mutating call (stop_streams, resolved as a no-op -- it never
        # itself calls _restart_service) must still block on the module lock
        # `stream_topics` is holding, proving the lock spans the whole
        # mutating body, not just the _restart_service call.
        second_done = threading.Event()

        def call_stop_streams() -> None:
            control.stop_streams(channels=["nope.x"], events=None)
            second_done.set()

        second = threading.Thread(target=call_stop_streams)
        second.start()
        second.join(timeout=0.3)
        assert not second_done.is_set(), (
            "second mutating call ran before the first released the lock"
        )

        release.set()
        first.join(timeout=5)
        second.join(timeout=5)
        assert first_done.is_set()
        assert second_done.is_set()

    def test_fleet_status_never_blocks_on_a_held_lock(self, tmp_path: pathlib.Path) -> None:
        assert control._control_lock.acquire(blocking=False)
        try:
            # fleet_status is read-only and must not try to acquire the lock
            # at all -- if it did, this call would deadlock right here.
            status = control.fleet_status()
        finally:
            control._control_lock.release()
        assert status["service"] == "stopped"
