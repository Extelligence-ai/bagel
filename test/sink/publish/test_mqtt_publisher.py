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
        assert fake["client"].kwargs["client_id"] == "bagel/acme/r7"

    def test_client_id_is_deterministic_across_reconnects(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        # Codex review: reconnect displacement semantics depend on the same
        # (tenant, robot) pair always producing the same client id.
        p = _publisher()
        p.connect()
        first_id = fake["client"].kwargs["client_id"]
        p.connect()
        second_id = fake["client"].kwargs["client_id"]
        assert first_id == second_id == "bagel/acme/r7"

    def test_client_id_does_not_collide_across_tenant_robot_boundary(
        self, fake: dict[str, FakeFleetPaho], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex review (3909409399): the old "bagel-{tenant}-{robot}" format
        # collided on the hyphen -- ("acme-west", "r7") and ("acme",
        # "west-r7") both produced "bagel-acme-west-r7". "/" is outside both
        # id charsets, so the new delimiter is provably collision-free.
        _publisher(tenant="acme-west", robot="r7").connect()
        id_a = fake["client"].kwargs["client_id"]

        _publisher(tenant="acme", robot="west-r7").connect()
        id_b = fake["client"].kwargs["client_id"]

        assert id_a != id_b
        assert id_a == "bagel/acme-west/r7"
        assert id_b == "bagel/acme/west-r7"

    def test_client_id_suffix_defaults_to_empty_unchanged_live_id(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        """The live service's own publisher never passes `client_id_suffix`
        -- its id must stay exactly what it always was."""
        _publisher().connect()
        assert fake["client"].kwargs["client_id"] == "bagel/acme/r7"

    def test_client_id_suffix_is_appended(self, fake: dict[str, FakeFleetPaho]) -> None:
        """Codex round 3 follow-up (PR #214, P2, comment 3925391258): the
        selftest CLI passes `client_id_suffix="-selftest"` so its
        MqttPublisher never derives the SAME client id as the live
        service's own (same tenant/robot) -- a broker kicks the existing
        session when a new connection claims an already-connected client
        id, so without this the selftest would displace live streaming."""
        _publisher(client_id_suffix="-selftest").connect()
        assert fake["client"].kwargs["client_id"] == "bagel/acme/r7-selftest"

    def test_client_id_suffix_is_deterministic_across_reconnects(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        """Same determinism guarantee as the unsuffixed id: a suffixed
        client id must still be identical on every reconnect, not merely
        different from the live service's."""
        p = _publisher(client_id_suffix="-selftest")
        p.connect()
        first_id = fake["client"].kwargs["client_id"]
        p.connect()
        second_id = fake["client"].kwargs["client_id"]
        assert first_id == second_id == "bagel/acme/r7-selftest"

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

    @pytest.mark.parametrize("reason", ["stopped", "paused"])
    def test_close_reason_flows_into_clean_stop_payload(
        self, fake: dict[str, FakeFleetPaho], reason: str
    ) -> None:
        p = _publisher()
        p.connect()
        p.close(reason=reason)
        stop = [c for c in fake["client"].calls if c[0] == "publish"][-1]
        assert json.loads(stop[2])["reason"] == reason


class TestSetTls:
    """CRITICAL fix: a post-renewal reconnect must pick up the NEW cert/key paths.

    `MqttPublisher` captures `tls_certfile`/`tls_keyfile` at construction and
    re-reads them from `self._tls` on every `connect()`. Without a seam to
    replace that dict after a renewal rotates the files on disk, every
    reconnect after the old files are unlinked fails forever. `set_tls`
    fixes this by atomically swapping in a NEW dict (never mutating the old
    one in place, so a router thread reading `self._tls` mid-connect never
    sees a half-updated mix of old and new paths).
    """

    def test_set_tls_replaces_the_tls_dict_with_a_new_object(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher(
            tls_ca_certs="/old-ca.pem", tls_certfile="/old-c.pem", tls_keyfile="/old-k.pem"
        )
        old_tls = p._tls

        p.set_tls(tls_ca_certs="/new-ca.pem", tls_certfile="/new-c.pem", tls_keyfile="/new-k.pem")

        assert p._tls is not old_tls  # a NEW dict, not an in-place mutation
        assert p._tls == {
            "ca_certs": "/new-ca.pem",
            "certfile": "/new-c.pem",
            "keyfile": "/new-k.pem",
        }
        # the old dict object itself is untouched -- a reader still holding
        # it (e.g. mid-connect on another thread) never sees a partial update
        assert old_tls == {
            "ca_certs": "/old-ca.pem",
            "certfile": "/old-c.pem",
            "keyfile": "/old-k.pem",
        }

    def test_reconnect_after_set_tls_uses_the_new_paths(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher(
            tls_ca_certs="/old-ca.pem", tls_certfile="/old-c.pem", tls_keyfile="/old-k.pem"
        )
        p.connect()
        assert fake["client"].tls == {
            "ca_certs": "/old-ca.pem",
            "certfile": "/old-c.pem",
            "keyfile": "/old-k.pem",
        }

        p.set_tls(tls_ca_certs="/new-ca.pem", tls_certfile="/new-c.pem", tls_keyfile="/new-k.pem")
        p.connect()  # forced reconnect, e.g. after a broker drop

        assert fake["client"].tls == {
            "ca_certs": "/new-ca.pem",
            "certfile": "/new-c.pem",
            "keyfile": "/new-k.pem",
        }


class TestReconnectCounter:
    def test_on_disconnect_is_registered_at_connect(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        assert fake["client"].on_disconnect is not None

    def test_disconnect_callback_increments_reconnects(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher()
        p.connect()
        assert p.reconnects == 0
        client = fake["client"]
        # Simulate paho VERSION2 firing the callback (client, userdata, flags, rc, props).
        client.on_disconnect(client, None, object(), object(), None)
        assert p.reconnects == 1
        client.on_disconnect(client, None, object(), object(), None)
        assert p.reconnects == 2

    def test_clean_close_does_not_double_count_as_reconnect(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher()
        p.connect()
        # close() drives the fake client to a disconnected state; since our fake
        # doesn't synchronously invoke on_disconnect (paho's real client does,
        # once the network thread is stopped), simulate that here.
        client = fake["client"]

        def fake_disconnect() -> None:
            client.calls.append(("disconnect",))
            client._connected = False
            client.on_disconnect(client, None, object(), object(), None)

        client.disconnect = fake_disconnect  # type: ignore[method-assign]
        p.close()
        assert p.reconnects == 0

    def test_replace_path_teardown_does_not_count_as_reconnect(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher()
        p.connect()
        first = fake["client"]

        # Simulate paho's "socket still live" semantics: when the prior
        # client's TCP session is still up (e.g. we are replacing it after a
        # wait_for_publish timeout, not a broker-side drop), disconnect()
        # fires on_disconnect synchronously -- same as the clean-close case
        # above. connect()'s replace-path teardown must gate this the same
        # way close() does.
        def fake_disconnect() -> None:
            first.calls.append(("disconnect",))
            first._connected = False
            first.on_disconnect(first, None, object(), object(), None)

        first.disconnect = fake_disconnect  # type: ignore[method-assign]
        p.connect()  # replace-path: tears down `first` before building the new client

        assert p.reconnects == 0

    def test_genuine_drop_outside_our_own_teardown_still_counts(
        self, fake: dict[str, FakeFleetPaho]
    ) -> None:
        p = _publisher()
        p.connect()
        client = fake["client"]
        # An ordinary broker-drop callback -- not fired from within our own
        # connect()/close() teardown -- must still be counted; the _closing
        # gate must not swallow this too.
        client.on_disconnect(client, None, object(), object(), None)
        assert p.reconnects == 1

    def test_handler_exceptions_are_swallowed(self, fake: dict[str, FakeFleetPaho]) -> None:
        p = _publisher()
        p.connect()
        client = fake["client"]

        class Boom:
            def __add__(self, other: object) -> "Boom":
                raise RuntimeError("boom")

        p.reconnects = Boom()  # force the increment inside the handler to raise
        client.on_disconnect(client, None, object(), object(), None)  # must not raise


def test_mqtt_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.mqtt", raising=False)
    importlib.import_module("src.sink.publish.mqtt")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)
