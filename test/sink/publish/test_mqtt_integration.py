"""Round-trip integration tests against a real MQTT broker (spec §8).

Gated on MQTT_TEST_BROKER (e.g. mqtt://localhost:1883). In CI these run in
the `iot` job against a mosquitto broker container; locally:
    docker run -d -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
"""

import json
import os
import queue
import time
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from src.sink.publish.mqtt import MqttPublisher

BROKER = os.environ.get("MQTT_TEST_BROKER")
pytestmark = pytest.mark.skipif(not BROKER, reason="MQTT_TEST_BROKER not set")


@pytest.fixture()
def subscriber() -> Iterator[tuple[object, "queue.Queue[tuple[str, bytes, bool]]"]]:
    import paho.mqtt.client as paho

    inbox: queue.Queue = queue.Queue()
    client = paho.Client(
        callback_api_version=paho.CallbackAPIVersion.VERSION2,
        client_id=f"sub-{uuid.uuid4().hex[:8]}",
        protocol=paho.MQTTv5,
    )
    client.on_message = lambda cl, ud, msg: inbox.put((msg.topic, msg.payload, msg.retain))
    from urllib.parse import urlparse

    parsed = urlparse(BROKER)
    client.connect(parsed.hostname, parsed.port or 1883)
    client.loop_start()
    yield client, inbox
    client.loop_stop()
    client.disconnect()


def _drain(
    inbox: "queue.Queue[tuple[str, bytes, bool]]", topic: str, timeout: float = 5.0
) -> tuple[str, bytes, bool] | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            got = inbox.get(timeout=deadline - time.time())
        except queue.Empty:
            return None
        if got[0] == topic:
            return got
    return None


def _publisher(tenant: str, robot: str) -> "MqttPublisher":
    from src.sink.publish.mqtt import MqttPublisher

    return MqttPublisher(BROKER, tenant, robot)


def _subscribe_and_wait(client: object, topic: str) -> tuple[int, int]:
    result = client.subscribe(topic, qos=1)
    time.sleep(0.3)
    return result


def test_channels_round_trip(
    subscriber: tuple[object, "queue.Queue[tuple[str, bytes, bool]]"],
) -> None:
    client, inbox = subscriber
    tenant, robot = "it", uuid.uuid4().hex[:8]
    topic = f"bagel/v1/{tenant}/{robot}/channels"
    _subscribe_and_wait(client, topic)

    p = _publisher(tenant, robot)
    p.connect()
    p.publish_channels({"v": 1, "seq": 1, "t_batch": time.time(), "samples": []})
    got = _drain(inbox, topic)
    p.close()
    assert got is not None, "channels batch never arrived"
    assert json.loads(got[1])["seq"] == 1


def test_schema_is_retained_for_late_subscriber(
    subscriber: tuple[object, "queue.Queue[tuple[str, bytes, bool]]"],
) -> None:
    client, inbox = subscriber
    tenant, robot = "it", uuid.uuid4().hex[:8]
    topic = f"bagel/v1/{tenant}/{robot}/schema"

    p = _publisher(tenant, robot)
    p.connect()
    p.publish_schema({"v": 1, "channels": [{"c": "imu.ax", "type": "number"}]})
    time.sleep(0.5)  # let the broker store the retained message
    _subscribe_and_wait(client, topic)  # subscribe AFTER publish
    got = _drain(inbox, topic)
    p.close()
    assert got is not None, "retained schema not delivered to late subscriber"
    assert got[2] is True or json.loads(got[1])["v"] == 1  # paho v5 retain flag on stored delivery


def test_clean_close_publishes_stopped_heartbeat(
    subscriber: tuple[object, "queue.Queue[tuple[str, bytes, bool]]"],
) -> None:
    client, inbox = subscriber
    tenant, robot = "it", uuid.uuid4().hex[:8]
    topic = f"bagel/v1/{tenant}/{robot}/heartbeat"
    _subscribe_and_wait(client, topic)

    p = _publisher(tenant, robot)
    p.connect()
    p.publish_heartbeat({"v": 1, "t": time.time(), "online": True})
    online = _drain(inbox, topic)
    p.close()
    stopped = _drain(inbox, topic)
    assert online and json.loads(online[1])["online"] is True
    assert stopped is not None, "clean close never published a stopped heartbeat"
    body = json.loads(stopped[1])
    assert body["online"] is False and body["reason"] == "stopped"


def test_lwt_fires_on_unclean_disconnect(
    subscriber: tuple[object, "queue.Queue[tuple[str, bytes, bool]]"],
) -> None:
    client, inbox = subscriber
    tenant, robot = "it", uuid.uuid4().hex[:8]
    topic = f"bagel/v1/{tenant}/{robot}/heartbeat"
    _subscribe_and_wait(client, topic)

    p = _publisher(tenant, robot)
    p.connect()
    # Simulate a crash: sever the socket without DISCONNECT so the broker
    # publishes the will. paho's socket is reachable via the private client;
    # loop_stop first so the loop doesn't race the reconnect.
    p._client.loop_stop()
    p._client._sock_close()  # deliberate unclean kill (private paho API)
    got = _drain(inbox, topic, timeout=10.0)
    assert got is not None, "LWT never fired"
    body = json.loads(got[1])
    assert body == {"v": 1, "online": False, "reason": "lwt"}
