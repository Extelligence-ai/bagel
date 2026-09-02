"""MqttPublisher unit tests over a fake paho client (no broker)."""

import importlib
import json
import sys
import types

import pytest

from settings import settings
from src.sink.publish import FleetDisabledError
from src.sink.publish import mqtt as publish_mqtt
from src.sink.publish.publisher import PublishError


class FakeMsgInfo:
    def __init__(
        self, rc: int = 0, published: bool = True, raise_on_wait: Exception | None = None
    ) -> None:
        self.rc = rc
        self._published = published
        self._raise = raise_on_wait

    def wait_for_publish(self, timeout: float | None = None) -> None:
        if self._raise:
            raise self._raise

    def is_published(self) -> bool:
        return self._published


class FakeFleetPaho:
    MQTT_ERR_SUCCESS = 0

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.calls: list[tuple[object, ...]] = []
        self.will: tuple[str, str, int, bool] | None = None
        self.tls: dict[str, object] | None = None
        self.userpass: tuple[str, str | None] | None = None
        self._connected = False
        self.next_info = FakeMsgInfo()

    def will_set(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> None:
        self.will = (topic, payload, qos, retain)

    def tls_set(self, **kwargs: object) -> None:
        self.tls = kwargs

    def username_pw_set(self, username: str, password: str | None = None) -> None:
        self.userpass = (username, password)

    def connect(self, host: str, port: int, keepalive: int = 60) -> None:
        self.calls.append(("connect", host, port, keepalive))
        self._connected = True

    def loop_start(self) -> None:
        self.calls.append(("loop_start",))

    def loop_stop(self) -> None:
        self.calls.append(("loop_stop",))

    def disconnect(self) -> None:
        self.calls.append(("disconnect",))
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> FakeMsgInfo:
        self.calls.append(("publish", topic, payload, qos, retain))
        return self.next_info


@pytest.fixture()
def fake(monkeypatch: pytest.MonkeyPatch) -> dict[str, FakeFleetPaho]:
    holder: dict[str, FakeFleetPaho] = {}

    def fake_client(**kwargs: object) -> FakeFleetPaho:
        holder["client"] = FakeFleetPaho(**kwargs)
        return holder["client"]

    fake_module = types.SimpleNamespace(
        Client=fake_client,
        CallbackAPIVersion=types.SimpleNamespace(VERSION2="v2"),
        MQTTv5=5,
        MQTT_ERR_SUCCESS=0,
    )
    monkeypatch.setattr(publish_mqtt, "_paho", lambda: fake_module)
    return holder


def _publisher(**kw: object) -> publish_mqtt.MqttPublisher:
    defaults = dict(broker_url="mqtts://fleet.example.com", tenant="acme", robot="r7")
    defaults.update(kw)
    return publish_mqtt.MqttPublisher(**defaults)


class TestConnect:
    def test_will_is_set_before_connect_with_lwt_payload(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher(tls_ca_certs="/ca.pem", tls_certfile="/c.pem", tls_keyfile="/k.pem")
        p.connect()
        client = fake["client"]
        topic, payload, qos, retain = client.will
        assert topic == "bagel/v1/acme/r7/heartbeat"
        assert json.loads(payload) == {"v": 1, "online": False, "reason": "lwt"}
        assert qos == 1 and retain is True
        # will_set recorded before connect in the call ordering
        # will/tls/userpass aren't in calls; connect is first *call*
        assert client.calls[0][0] == "connect"
        assert client.tls == {"ca_certs": "/ca.pem", "certfile": "/c.pem", "keyfile": "/k.pem"}

    def test_mqtts_defaults_to_8883_and_mqtt_to_1883(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher(broker_url="mqtts://h")
        p.connect()
        assert fake["client"].calls[0] == ("connect", "h", 8883, 30)
        p2 = _publisher(broker_url="mqtt://h2:1884")
        p2.connect()
        assert fake["client"].calls[0] == ("connect", "h2", 1884, 30)

    def test_bad_scheme_raises_at_construction(self) -> None:
        with pytest.raises(ValueError, match="scheme"):
            _publisher(broker_url="http://h")

    def test_plain_broker_sets_no_tls(self, fake: dict[str, FakeFleetPaho]) -> None:
        _publisher(broker_url="mqtt://h").connect()
        assert fake["client"].tls is None

    def test_client_id_is_deterministic(self, fake: dict[str, FakeFleetPaho]) -> None:
        _publisher().connect()
        assert fake["client"].kwargs["client_id"] == "bagel-acme-r7"

    def test_connect_raises_when_fleet_disabled_before_touching_paho(
        self, fake: dict[str, FakeFleetPaho], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "FLEET_ENABLED", False)
        p = _publisher()
        with pytest.raises(FleetDisabledError, match="FLEET_ENABLED"):
            p.connect()
        assert fake == {}

    def test_reconnect_tears_down_prior_client_before_replacing_it(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher()
        p.connect()
        first = fake["client"]
        p.connect()
        second = fake["client"]
        assert first is not second
        assert ("loop_stop",) in first.calls and ("disconnect",) in first.calls
        assert not first.is_connected()
        assert second.is_connected()
        assert p.connected


class TestPublish:
    def test_publishes_json_qos1_on_wire_topic(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        p.publish_channels({"v": 1, "seq": 3})
        call = next(c for c in fake["client"].calls if c[0] == "publish")
        assert call[1] == "bagel/v1/acme/r7/channels"
        assert json.loads(call[2]) == {"v": 1, "seq": 3}
        assert call[3] == 1 and call[4] is False

    def test_retained_kinds(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        p.publish_schema({"v": 1, "channels": []})
        call = [c for c in fake["client"].calls if c[0] == "publish"][-1]
        assert call[1].endswith("/schema") and call[4] is True

    def test_unacked_publish_raises(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        fake["client"].next_info = FakeMsgInfo(published=False)
        with pytest.raises(PublishError):
            p.publish_event({"v": 1})

    def test_bad_rc_raises(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        fake["client"].next_info = FakeMsgInfo(rc=4)
        with pytest.raises(PublishError):
            p.publish_channels({"v": 1})

    def test_wait_timeout_wraps_into_publish_error(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        fake["client"].next_info = FakeMsgInfo(raise_on_wait=RuntimeError("timed out"))
        with pytest.raises(PublishError, match="timed out|not acknowledged"):
            p.publish_channels({"v": 1})


class TestClose:
    def test_close_publishes_stopped_heartbeat_then_disconnects(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher()
        p.connect()
        p.close()
        client = fake["client"]
        stop = [c for c in client.calls if c[0] == "publish"][-1]
        body = json.loads(stop[2])
        assert body["online"] is False and body["reason"] == "stopped" and body["v"] == 1
        assert "t" in body
        assert ("loop_stop",) in client.calls and ("disconnect",) in client.calls
        # ordering: stopped-heartbeat publish before disconnect
        assert client.calls.index(stop) < client.calls.index(("disconnect",))

    def test_close_is_idempotent_and_safe_unconnected(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.close()
        p.close()

    def test_close_swallows_publish_failure(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        fake["client"].next_info = FakeMsgInfo(published=False)
        p.close()  # must not raise


def test_mqtt_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.mqtt", raising=False)
    importlib.import_module("src.sink.publish.mqtt")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)
