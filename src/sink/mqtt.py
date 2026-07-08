"""Provide a topic sink that subscribes to an MQTT broker.

Pure Python (paho-mqtt); no robotics middleware required. MQTT specifics handled here:

- **Topic discovery**: MQTT brokers expose no topic-listing API, so the sink subscribes
  to the ``#`` wildcard for a short window (``discovery_seconds``) and collects the
  topics it sees -- retained messages arrive immediately, live ones during the window.
  Topics not seen during discovery can still be subscribed to explicitly.
- **Schemas**: payloads are JSON in the vast majority of deployments, so structure is
  inferred from sampled payloads. Bare JSON scalars/arrays are wrapped as
  ``{"value": ...}`` and non-JSON payloads as ``{"payload": "<text>"}``, so every topic
  gets a queryable struct. (Sparkplug B / protobuf payloads are a future extension.)
- **Timestamps**: MQTT messages carry no standard timestamp; arrival time is used
  (the `TopicBufferWriter` default).
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

import pyarrow as pa
from paho.mqtt import client as paho

from settings import settings
from src.di import module
from src.pipeline.base import Pipeline
from src.sink import base, buffer
from src.sink.sparkplug import decode as sparkplug

DISCOVERY_WILDCARD = "#"

TIMESTAMP_DIVISORS = {
    "second": 1.0,
    "millisecond": 1e3,
    "microsecond": 1e6,
    "nanosecond": 1e9,
}


def normalize_payload(payload: bytes) -> dict[str, Any]:
    """Normalize a raw MQTT payload into a JSON-serializable dictionary.

    JSON objects pass through; bare JSON scalars and arrays are wrapped as
    ``{"value": ...}``; anything that is not valid JSON becomes ``{"payload": "<text>"}``.
    """
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"payload": payload.decode("utf-8", errors="replace")}
    if isinstance(decoded, dict):
        return decoded
    return {"value": decoded}


def infer_struct(samples: list[dict[str, Any]]) -> pa.StructType:
    """Infer a PyArrow StructType from sampled payload dictionaries.

    Fields are unified across all samples (PyArrow's from_pylist only looks at the
    first row, so missing keys are padded with None to make them nullable fields).
    """
    if not samples:
        raise ValueError("Cannot infer a schema from zero samples.")
    keys: dict[str, None] = {}  # ordered union of keys across samples
    for sample in samples:
        keys.update(dict.fromkeys(sample))
    padded = [{key: sample.get(key) for key in keys} for sample in samples]
    table = pa.Table.from_pylist(padded)
    return pa.struct(table.schema)


class TopicSink(base.TopicSink):
    """A topic sink that subscribes to an MQTT broker.

    Works with any MQTT 3.1.1/5.0 broker (Mosquitto, EMQX, HiveMQ, AWS IoT Core, ...).
    """

    def __init__(  # noqa: PLR0913
        self,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        discovery_seconds: float = 2.0,
        sample_size: int = 5,
        schema_timeout_seconds: float = 5.0,
        client_id: str | None = None,
        transport: str = "tcp",
        tls: bool = False,
        tls_ca_certs: str | None = None,
        tls_insecure: bool = False,
        timestamp_field: str | None = None,
        timestamp_unit: str = "second",
        sparkplug_b: bool = True,
    ) -> None:
        """Initialize the MQTT topic sink.

        Args:
            host (str): The hostname of the MQTT broker.
            port (int): The port number of the MQTT broker (typically 1883; 8883 for TLS;
                often 443/8083 for websockets).
            username (str | None, optional): Username for broker authentication.
            password (str | None, optional): Password for broker authentication.
            discovery_seconds (float, optional): How long to listen on the ``#`` wildcard
                to discover topics and sample payloads. Retained messages arrive
                immediately; larger values catch more low-rate topics. Defaults to 2.0.
            sample_size (int, optional): Maximum payload samples kept per topic for
                schema inference. Defaults to 5.
            schema_timeout_seconds (float, optional): When subscribing to a topic that was
                not seen during discovery, how long to wait for a first message to infer
                its schema before falling back to a raw-payload schema. Defaults to 5.0.
            client_id (str | None, optional): MQTT client identifier. If None, a random
                one is generated.
            transport (str, optional): "tcp" or "websockets" (for brokers like AWS IoT
                Core behind websockets). Defaults to "tcp".
            tls (bool, optional): If True, connect over TLS. Defaults to False.
            tls_ca_certs (str | None, optional): Path to a CA bundle for TLS. If None,
                the system defaults are used. Defaults to None.
            tls_insecure (bool, optional): If True, skip server certificate verification
                (testing only). Defaults to False.
            timestamp_field (str | None, optional): A payload field carrying the message
                timestamp (e.g. "ts"). If set, buffered messages use it instead of
                arrival time; messages missing the field fall back to arrival time.
                Defaults to None (arrival time).
            timestamp_unit (str, optional): The unit of `timestamp_field`: "second",
                "millisecond", "microsecond", or "nanosecond". Defaults to "second".
            sparkplug_b (bool, optional): If True, payloads on `spBv1.0/` topics are
                decoded as Sparkplug B: scalar metrics flatten to named fields, plus
                `timestamp` (epoch ms) and `seq`. Pair with
                `timestamp_field="timestamp", timestamp_unit="millisecond"` to use the
                Sparkplug timestamps for buffering. Defaults to True.

        Raises:
            ValueError: If 'transport' or 'timestamp_unit' is invalid.

        """
        if transport not in ("tcp", "websockets"):
            raise ValueError(f"'transport' must be 'tcp' or 'websockets', got {transport!r}.")
        if timestamp_unit not in TIMESTAMP_DIVISORS:
            raise ValueError(
                f"'timestamp_unit' must be one of {sorted(TIMESTAMP_DIVISORS)}, "
                f"got {timestamp_unit!r}."
            )

        self._paho = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            transport=transport,
        )
        if username is not None:
            self._paho.username_pw_set(username, password)
        if tls:
            self._paho.tls_set(ca_certs=tls_ca_certs)
            if tls_insecure:
                self._paho.tls_insecure_set(True)
        self._paho.on_message = self._on_discovery_message

        self._discovery_seconds = discovery_seconds
        self._sample_size = sample_size
        self._schema_timeout_seconds = schema_timeout_seconds
        self._timestamp_field = timestamp_field
        self._timestamp_divisor = TIMESTAMP_DIVISORS[timestamp_unit]
        self._sparkplug_b = sparkplug_b

        self._samples: dict[str, list[dict[str, Any]]] = {}
        self._samples_lock = threading.Lock()
        self._callbacks: dict[str, Any] = {}  # topic -> paho callback (for unsubscribe)

        # The base __init__ calls _connect() before assigning self._host/_port,
        # so keep our own copy of the broker address (same pattern as the ros bridges).
        self._broker = (host, int(port))

        super().__init__(host, port)  # establishes the connection, runs discovery

    # -- paho wiring ---------------------------------------------------------------

    def _connect(self) -> None:
        if self._paho.is_connected():
            return
        self._paho.connect(self._broker[0], self._broker[1], keepalive=60)
        self._paho.loop_start()  # background network thread

    def _disconnect(self) -> None:
        self._paho.loop_stop()
        self._paho.disconnect()

    def _normalize(self, topic: str, payload: bytes) -> dict[str, Any]:
        """Normalize a payload, decoding Sparkplug B on its namespace when enabled."""
        if self._sparkplug_b and sparkplug.is_sparkplug_topic(topic):
            try:
                return sparkplug.decode_payload(payload)
            except Exception:
                logging.warning(
                    "Failed to decode '%s' as Sparkplug B; treating payload as JSON/text",
                    topic,
                )
        return normalize_payload(payload)

    def _on_discovery_message(self, client: object, userdata: object, message: object) -> None:
        """Collect topic names and payload samples from the discovery wildcard."""
        with self._samples_lock:
            samples = self._samples.setdefault(message.topic, [])
            if len(samples) < self._sample_size:
                samples.append(self._normalize(message.topic, message.payload))

    def _available_topics(self) -> list[str]:
        """Discover topics by listening on the wildcard for the discovery window."""
        self._paho.subscribe(DISCOVERY_WILDCARD)
        time.sleep(self._discovery_seconds)
        self._paho.unsubscribe(DISCOVERY_WILDCARD)
        with self._samples_lock:
            return sorted(self._samples)

    def _sample(self, topic: str) -> list[dict[str, Any]]:
        """Return payload samples for a topic, waiting briefly for one if unseen."""
        with self._samples_lock:
            if self._samples.get(topic):
                return list(self._samples[topic])

        # Topic unseen during discovery: listen for a first message to infer its schema.
        self._paho.subscribe(topic)
        deadline = time.monotonic() + self._schema_timeout_seconds
        while time.monotonic() < deadline:
            with self._samples_lock:
                if self._samples.get(topic):
                    return list(self._samples[topic])
            time.sleep(0.05)
        logging.warning(
            "No message received on '%s' within %.1fs; using raw-payload schema",
            topic,
            self._schema_timeout_seconds,
        )
        return [{"payload": ""}]

    def _type_name(self, topic: str) -> str:
        if self._sparkplug_b and sparkplug.is_sparkplug_topic(topic):
            return "mqtt/sparkplugb"
        return "mqtt/json"

    def _definition(self, topic: str) -> str:
        """Return a sample payload as the topic's human-readable definition."""
        samples = self._sample(topic)
        return json.dumps(samples[0], indent=2, default=str)

    def _struct(self, topic: str) -> pa.StructType:
        return infer_struct(self._sample(topic))

    def _extract_payload_timestamp(self, message: dict[str, Any]) -> float:
        """Read the configured timestamp field, falling back to arrival time."""
        value = message.get(self._timestamp_field)
        if isinstance(value, int | float) and not isinstance(value, bool):
            return float(value) / self._timestamp_divisor
        return time.time()

    def subscribe(
        self,
        topic: str,
        pipeline: Pipeline | None = None,
        overwrite: bool = False,
        buffer_size_bytes: int | None = settings.JSONL_BUFFER_SIZE_PER_TOPIC_BYTES,
        extract_timestamp: Callable[[dict[str, Any]], float] | None = None,
    ) -> None:
        """Subscribe to a topic, defaulting timestamps to the configured payload field."""
        if extract_timestamp is None and self._timestamp_field is not None:
            extract_timestamp = self._extract_payload_timestamp
        super().subscribe(
            topic,
            pipeline=pipeline,
            overwrite=overwrite,
            buffer_size_bytes=buffer_size_bytes,
            extract_timestamp=extract_timestamp,
        )

    def _subscribe(self, writer: buffer.TopicBufferWriter) -> None:
        def _on_message(client: object, userdata: object, message: object) -> None:
            try:
                writer.append(self._normalize(message.topic, message.payload))
            except Exception:
                # A malformed payload must not kill the subscription's network thread.
                logging.exception("Failed to buffer message on topic '%s'", message.topic)

        self._callbacks[writer.topic] = _on_message
        self._paho.message_callback_add(writer.topic, _on_message)
        self._paho.subscribe(writer.topic)

    def _unsubscribe(self, writer: buffer.TopicBufferWriter) -> None:
        if writer.topic in self._callbacks:
            self._paho.message_callback_remove(writer.topic)
            self._paho.unsubscribe(writer.topic)
            del self._callbacks[writer.topic]


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = TopicSink
