"""Decode Sparkplug B payloads into flat, queryable dictionaries.

Sparkplug B (Eclipse Sparkplug) is the structured MQTT standard used in industrial
IoT: protobuf payloads carrying a list of named metrics, published under the
``spBv1.0/`` topic namespace. This decoder flattens a payload's scalar metrics into
``{metric_name: value, ...}`` plus the payload ``timestamp`` (epoch milliseconds) and
``seq``, so the standard schema inference and SQL work unchanged.

Complex metric values (DataSet, Template) are not decoded in v1: protobuf skips the
unknown fields, so such metrics simply appear with a null value.
"""

import logging
from typing import Any

from bagel.sink.sparkplug import sparkplug_b_minimal_pb2 as pb

SPARKPLUG_NAMESPACE = "spBv1.0/"


def is_sparkplug_topic(topic: str) -> bool:
    """Return True if the topic is in the Sparkplug B namespace."""
    return topic.startswith(SPARKPLUG_NAMESPACE)


def _metric_name(metric: pb.Payload.Metric) -> str | None:
    """Return the metric's name, or an alias-based fallback (NDATA often omits names)."""
    if metric.HasField("name"):
        return metric.name
    if metric.HasField("alias"):
        return f"alias_{metric.alias}"
    return None


def _metric_value(metric: pb.Payload.Metric) -> Any:  # noqa: ANN401 -- metric values are heterogeneous
    """Return the metric's scalar value, or None for null/complex metrics."""
    which = metric.WhichOneof("value")
    if metric.is_null or which is None:
        return None
    value = getattr(metric, which)
    if which == "bytes_value":
        return value.decode("utf-8", errors="replace")
    return value


def decode_payload(payload: bytes) -> dict[str, Any]:
    """Decode a Sparkplug B payload into a flat dictionary.

    Returns:
        ``{"timestamp": <epoch ms>, "seq": <n>, <metric name>: <value>, ...}``.
        Metrics whose value is null or of an undecoded complex type map to None.

    Raises:
        Exception: If the bytes are not a valid protobuf payload.

    """
    message = pb.Payload()
    message.ParseFromString(payload)

    decoded: dict[str, Any] = {}
    if message.HasField("timestamp"):
        decoded["timestamp"] = message.timestamp
    if message.HasField("seq"):
        decoded["seq"] = message.seq

    for metric in message.metrics:
        name = _metric_name(metric)
        if name is None:
            logging.debug("Skipping Sparkplug metric with neither name nor alias")
            continue
        decoded[name] = _metric_value(metric)

    return decoded
