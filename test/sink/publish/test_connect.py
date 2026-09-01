"""Connection policy: identity + streams config -> MqttPublisher kwargs.

``resolve_publisher_kwargs`` picks a broker (streams.broker wins over
identity.broker_url), then enforces the transport policy: ``mqtts://``
requires an enrolled identity (TLS material comes from its cert paths);
``mqtt://`` (plaintext) requires both ``settings.FLEET_DEV_INSECURE`` and a
loopback/private host, so a misconfigured production robot can never fall
back to an unauthenticated, unencrypted broker.

``_is_local_or_private`` never does real DNS in these tests --
``socket.getaddrinfo`` is monkeypatched for every non-literal hostname case.
"""

import importlib
import pathlib
import socket
import sys
from inspect import Parameter, signature

import pytest

from settings import settings
from src.sink.publish import FleetNotEnrolledError, StreamConfigError
from src.sink.publish import connect as connect_mod
from src.sink.publish.config import StreamsConfig
from src.sink.publish.identity import Identity
from src.sink.publish.mqtt import MqttPublisher


def _identity(*, broker_url: str = "mqtts://fleet.example.com") -> Identity:
    return Identity(
        tenant="acme",
        robot_id="r2d2",
        broker_url=broker_url,
        enroll_url="https://enroll.example.com",
        expires_at="2027-01-01T00:00:00Z",
        key_path=pathlib.Path("/identity/robot.key"),
        cert_path=pathlib.Path("/identity/robot.crt"),
        ca_path=pathlib.Path("/identity/ca.crt"),
    )


def _streams(broker: str | None = None) -> StreamsConfig:
    return StreamsConfig(broker=broker)


def _mqtt_publisher_param_names() -> set[str]:
    params = signature(MqttPublisher.__init__).parameters
    return {name for name in params if name != "self"}


class TestMqttsHappyPath:
    def test_mqtts_with_identity_returns_tls_kwargs(self) -> None:
        identity = _identity(broker_url="mqtts://ignored.example.com")
        streams = _streams(broker="mqtts://fleet.example.com")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, identity)
        assert kwargs == {
            "broker_url": "mqtts://fleet.example.com",
            "tenant": "acme",
            "robot": "r2d2",
            "tls_ca_certs": "/identity/ca.crt",
            "tls_certfile": "/identity/robot.crt",
            "tls_keyfile": "/identity/robot.key",
        }

    def test_mqtts_broker_falls_back_to_identity_broker_url(self) -> None:
        identity = _identity(broker_url="mqtts://from-identity.example.com")
        streams = _streams(broker=None)
        kwargs = connect_mod.resolve_publisher_kwargs(streams, identity)
        assert kwargs["broker_url"] == "mqtts://from-identity.example.com"
        assert kwargs["tenant"] == "acme"
        assert kwargs["robot"] == "r2d2"


class TestMqttsRequiresIdentity:
    def test_mqtts_without_identity_raises_not_enrolled(self) -> None:
        streams = _streams(broker="mqtts://fleet.example.com")
        with pytest.raises(FleetNotEnrolledError, match="mqtts"):
            connect_mod.resolve_publisher_kwargs(streams, None)


class TestNoBrokerAnywhere:
    def test_neither_streams_nor_identity_broker_raises_naming_both_paths(self) -> None:
        streams = _streams(broker=None)
        with pytest.raises(FleetNotEnrolledError, match="streams.broker") as excinfo:
            connect_mod.resolve_publisher_kwargs(streams, None)
        assert "enroll" in str(excinfo.value).lower()

    def test_neither_with_identity_missing_broker_url_also_raises(self) -> None:
        # identity.broker_url is required non-empty by load_identity/enroll, but the
        # policy layer must not assume that -- an empty string is "unset" too.
        identity = _identity(broker_url="")
        streams = _streams(broker=None)
        with pytest.raises(FleetNotEnrolledError, match="streams.broker"):
            connect_mod.resolve_publisher_kwargs(streams, identity)


class TestMqttRequiresDevInsecure:
    def test_mqtt_without_dev_flag_raises_naming_setting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", False)
        streams = _streams(broker="mqtt://localhost")
        with pytest.raises(StreamConfigError, match="FLEET_DEV_INSECURE"):
            connect_mod.resolve_publisher_kwargs(streams, None)


class TestMqttDevInsecureHostPolicy:
    def test_localhost_literal_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        streams = _streams(broker="mqtt://localhost")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, None)
        assert kwargs["broker_url"] == "mqtt://localhost"
        assert kwargs["tenant"] == "dev"
        assert kwargs["robot"] == "robot"

    def test_private_ip_literal_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        streams = _streams(broker="mqtt://192.168.1.5")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, None)
        assert kwargs["broker_url"] == "mqtt://192.168.1.5"

    def test_public_ip_literal_raises_naming_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        streams = _streams(broker="mqtt://8.8.8.8")
        with pytest.raises(StreamConfigError, match="8.8.8.8"):
            connect_mod.resolve_publisher_kwargs(streams, None)

    def test_hostname_resolving_public_raises_via_fake_getaddrinfo(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)

        def fake_getaddrinfo(host: str, port: object, *args: object, **kwargs: object) -> list:
            assert host == "broker.example.com"
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        streams = _streams(broker="mqtt://broker.example.com")
        with pytest.raises(StreamConfigError, match="broker.example.com"):
            connect_mod.resolve_publisher_kwargs(streams, None)

    def test_hostname_resolving_private_ok_via_fake_getaddrinfo(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)

        def fake_getaddrinfo(host: str, port: object, *args: object, **kwargs: object) -> list:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.7", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        streams = _streams(broker="mqtt://broker.internal")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, None)
        assert kwargs["broker_url"] == "mqtt://broker.internal"

    def test_mqtt_with_identity_uses_identity_tenant_and_robot(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        identity = _identity(broker_url="mqtts://unused.example.com")
        streams = _streams(broker="mqtt://localhost")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, identity)
        assert kwargs == {
            "broker_url": "mqtt://localhost",
            "tenant": "acme",
            "robot": "r2d2",
        }


class TestIsLocalOrPrivate:
    def test_localhost_literal(self) -> None:
        assert connect_mod._is_local_or_private("localhost") is True

    def test_loopback_ip_literal(self) -> None:
        assert connect_mod._is_local_or_private("127.0.0.1") is True

    def test_private_ipv4_literal(self) -> None:
        assert connect_mod._is_local_or_private("10.1.2.3") is True
        assert connect_mod._is_local_or_private("192.168.0.1") is True

    def test_public_ip_literal(self) -> None:
        assert connect_mod._is_local_or_private("8.8.8.8") is False

    def test_hostname_all_private_via_fake_getaddrinfo(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_getaddrinfo(host: str, port: object, *args: object, **kwargs: object) -> list:
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.2", 0)),
            ]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        assert connect_mod._is_local_or_private("multi.internal") is True

    def test_hostname_mixed_private_and_public_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_getaddrinfo(host: str, port: object, *args: object, **kwargs: object) -> list:
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 0)),
            ]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        assert connect_mod._is_local_or_private("mixed.internal") is False

    def test_resolution_failure_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_getaddrinfo(host: str, port: object, *args: object, **kwargs: object) -> list:
            raise socket.gaierror("nope")

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        assert connect_mod._is_local_or_private("nowhere.example.com") is False

    def test_empty_resolution_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [])
        assert connect_mod._is_local_or_private("empty.example.com") is False


class TestKwargShapeMatchesMqttPublisher:
    def test_mqtts_kwargs_are_subset_of_real_signature(self) -> None:
        allowed = _mqtt_publisher_param_names()
        identity = _identity()
        streams = _streams(broker="mqtts://fleet.example.com")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, identity)
        assert set(kwargs) <= allowed
        # broker_url, tenant, robot are positional-or-keyword in MqttPublisher;
        # confirm none of our keys were dropped from its actual signature.
        params = signature(MqttPublisher.__init__).parameters
        for name in kwargs:
            assert params[name].kind in (
                Parameter.POSITIONAL_OR_KEYWORD,
                Parameter.KEYWORD_ONLY,
            )

    def test_mqtt_dev_kwargs_are_subset_of_real_signature(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_DEV_INSECURE", True)
        allowed = _mqtt_publisher_param_names()
        streams = _streams(broker="mqtt://localhost")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, None)
        assert set(kwargs) <= allowed

    def test_kwargs_construct_a_real_mqttpublisher(self) -> None:
        identity = _identity()
        streams = _streams(broker="mqtts://fleet.example.com")
        kwargs = connect_mod.resolve_publisher_kwargs(streams, identity)
        # Not connecting -- just proving the returned dict is accepted as-is.
        publisher = MqttPublisher(**kwargs)
        assert publisher is not None


def test_connect_module_does_not_import_paho_or_cryptography_eagerly(
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
    monkeypatch.delitem(sys.modules, "src.sink.publish.connect", raising=False)
    importlib.import_module("src.sink.publish.connect")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )
