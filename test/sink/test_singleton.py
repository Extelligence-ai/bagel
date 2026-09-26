"""Constructing a sink for a (host, port) that is already live returns it untouched.

`list_live_topics` and `subscribe_live_topics` each construct the sink. The second call
used to re-run the subclass `__init__` on the live singleton, replacing its connected
client with a fresh, never-connected one: the subscription then received nothing.
"""

import itertools
import json
import pathlib

import pyarrow as pa
import pytest

from settings import settings
from src.sink import base
from src.sink.buffer import TopicBufferWriter

_ports = itertools.count(31000)


class _ClientSink(base.TopicSink):
    """A sink whose constructor builds a client, as the MQTT and ROS bridge sinks do."""

    constructed = 0

    def __init__(self, host: str, port: int, option: str = "first") -> None:
        type(self).constructed += 1
        self.client = object()
        self.option = option
        super().__init__(host, port)

    def _connect(self) -> None:
        pass

    def _disconnect(self) -> None:
        pass

    def _available_topics(self) -> list[str]:
        return ["/a"]

    def _type_name(self, topic: str) -> str:
        return "t"

    def _definition(self, topic: str) -> str:
        return "float64 x"

    def _struct(self, topic: str) -> pa.StructType:
        return pa.struct([pa.field("x", pa.float64())])

    def _subscribe(self, writer: TopicBufferWriter) -> None:
        pass

    def _unsubscribe(self, writer: TopicBufferWriter) -> None:
        pass


@pytest.fixture(autouse=True)
def _isolated(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))


def test_a_second_construction_keeps_the_live_client() -> None:
    port = next(_ports)
    _ClientSink.constructed = 0
    first = _ClientSink("localhost", port)
    client = first.client
    second = _ClientSink("localhost", port, option="second")
    assert second is first
    assert second.client is client
    assert second.option == "first"
    assert _ClientSink.constructed == 1
    first.close()


def test_a_closed_sink_is_rebuilt_on_the_next_construction() -> None:
    port = next(_ports)
    first = _ClientSink("localhost", port)
    first.close()
    second = _ClientSink("localhost", port)
    assert second is not first
    second.close()


def test_mqtt_list_then_subscribe_receives_messages(
    make_sink: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("paho")
    from conftest import FakePahoClient

    from src.sink import mqtt

    sink = make_sink(retained={"plant/pump": [b'{"pressure": 4.2}']})  # list_live_topics
    connected = sink._paho
    assert sink.available_topics == ["plant/pump"]

    # subscribe_live_topics constructs the sink again; paho must not be rebuilt.
    monkeypatch.setattr(mqtt.paho, "Client", lambda **_: FakePahoClient())
    again = mqtt.TopicSink(host="broker.test", port=sink._broker[1], discovery_seconds=0.0)
    assert again is sink
    assert again._paho is connected

    again.subscribe("plant/pump")
    connected.deliver("plant/pump", json.dumps({"pressure": 3.9}).encode())
    assert again._buffers["plant/pump"].message_count >= 1


class _FlakySink(_ClientSink):
    """Fails after the base initializer ran, as a ROS bridge sink does on a rosapi error."""

    fail_next = True
    disconnected = 0

    def __init__(self, host: str, port: int) -> None:
        super().__init__(host, port)
        if type(self).fail_next:
            type(self).fail_next = False
            raise ConnectionError("rosapi did not answer")
        self.ready = True

    def _disconnect(self) -> None:
        type(self).disconnected += 1


def test_a_failed_subclass_init_leaves_no_half_built_singleton() -> None:
    port = next(_ports)
    _FlakySink.fail_next, _FlakySink.disconnected = True, 0
    with pytest.raises(ConnectionError):
        _FlakySink("localhost", port)
    assert ("localhost", port) not in base._global_sink_singletons
    assert _FlakySink.disconnected == 1  # its connection was not leaked
    retry = _FlakySink("localhost", port)
    assert retry.ready
    retry.close()
