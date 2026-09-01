"""Parse and validate the manifest's `streams:` section (fleet streaming).

Two phases: `load_streams` turns raw YAML into models (shape validation);
`StreamsConfig.resolve` binds channel rules to a topic's Arrow schema
(field existence, scalar-ness, wire types). No runtime behavior lives here.
"""

from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.types as pat
from pydantic import BaseModel, ConfigDict

from src.message.base import AccessPath
from src.sink.publish import StreamConfigError


def classify(pa_type: pa.DataType) -> str:
    """Wire-contract type name for a scalar Arrow type (spec §3)."""
    if pat.is_integer(pa_type) or pat.is_floating(pa_type):
        return "number"
    if pat.is_boolean(pa_type):
        return "bool"
    if pat.is_string(pa_type) or pat.is_large_string(pa_type):
        return "string"
    raise ValueError(f"{pa_type} is not a streamable scalar")


def resolve_path(struct: pa.StructType, dotted: str, *, field_label: str) -> AccessPath:
    """Walk a dotted path through nested structs to its leaf type."""
    current: pa.DataType = struct
    walked: list[str] = []
    for segment in dotted.split("."):
        if not pat.is_struct(current):
            raise StreamConfigError(
                field_label, f"'{'.'.join(walked)}' is not a struct; cannot descend to '{segment}'"
            )
        index = current.get_field_index(segment)
        if index == -1:
            raise StreamConfigError(field_label, f"unknown field '{segment}' in '{dotted}'")
        current = current.field(index).type
        walked.append(segment)
    return AccessPath(path=walked, pa_type=current)


MAX_RATE_HZ = 50.0
ARTIFACT_KINDS = ("mcap",)


class ChannelRule(BaseModel):
    """One `channels:` entry: project fields of a topic at a capped rate."""

    topic: str
    fields: list[str] | None = None
    geo: dict[str, str] | None = None
    rate_hz: float
    renames: dict[str, str] = {}

    @staticmethod
    def build(config: dict, label: str = "channels[]") -> "ChannelRule":
        """Build and validate a channel rule from config dict."""
        match config:
            case {"topic": str(topic), **rest}:
                pass
            case _:
                raise StreamConfigError(f"{label}.topic", f"missing or non-string topic: {config}")
        fields = rest.get("fields")
        geo = rest.get("geo")
        if (fields is None) == (geo is None):
            raise StreamConfigError(label, "exactly one of 'fields' or 'geo' is required")
        if fields is not None and (
            not isinstance(fields, list)
            or not fields
            or not all(isinstance(f, str) for f in fields)
        ):
            raise StreamConfigError(
                f"{label}.fields",
                f"must be a non-empty list of strings: {fields}",
            )
        if geo is not None:
            if not isinstance(geo, dict) or not {"lat", "lon"} <= set(geo):
                raise StreamConfigError(
                    f"{label}.geo",
                    f"requires 'lat' and 'lon' dotted paths: {geo}",
                )
            unknown = set(geo) - {"lat", "lon", "alt"}
            if unknown:
                raise StreamConfigError(f"{label}.geo", f"unknown keys {sorted(unknown)}")
        rate = rest.get("rate_hz")
        if (
            not isinstance(rate, int | float)
            or isinstance(rate, bool)
            or not 0 < rate <= MAX_RATE_HZ
        ):
            raise StreamConfigError(
                f"{label}.rate_hz",
                f"must be in (0, {MAX_RATE_HZ:g}]: {rate!r}",
            )
        renames = rest.get("as", {})
        if not isinstance(renames, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in renames.items()
        ):
            raise StreamConfigError(
                f"{label}.as",
                f"must map field paths to channel names: {renames}",
            )
        unknown = set(config) - {"topic", "fields", "geo", "rate_hz", "as"}
        if unknown:
            raise StreamConfigError(label, f"unknown keys {sorted(unknown)}")
        return ChannelRule(
            topic=topic,
            fields=fields,
            geo=geo,
            rate_hz=float(rate),
            renames=renames,
        )


class EventRule(BaseModel):
    """One `events:` entry: a named predicate with capture windows."""

    name: str
    topic: str
    predicate: str
    pre_seconds: float = 0.0
    post_seconds: float = 0.0
    debounce_seconds: float = 0.0
    artifact: str | None = None

    @staticmethod
    def build(config: dict, label: str = "events[]") -> "EventRule":
        """Build and validate an event rule from config dict."""
        match config:
            case {
                "name": str(name),
                "topic": str(topic),
                "predicate": str(predicate),
                **rest,
            }:
                pass
            case _:
                raise StreamConfigError(
                    label,
                    f"requires string 'name', 'topic' and 'predicate': {config}",
                )
        windows = {}
        for key in ("pre_seconds", "post_seconds", "debounce_seconds"):
            value = rest.get(key, 0.0)
            if not isinstance(value, int | float) or isinstance(value, bool) or value < 0:
                raise StreamConfigError(
                    f"{label}.{key}",
                    f"must be a non-negative number: {value!r}",
                )
            windows[key] = float(value)
        artifact = rest.get("artifact")
        if artifact is not None and artifact not in ARTIFACT_KINDS:
            raise StreamConfigError(
                f"{label}.artifact",
                f"must be one of {ARTIFACT_KINDS}: {artifact!r}",
            )
        unknown = set(config) - {
            "name",
            "topic",
            "predicate",
            "pre_seconds",
            "post_seconds",
            "debounce_seconds",
            "artifact",
        }
        if unknown:
            raise StreamConfigError(label, f"unknown keys {sorted(unknown)}")
        return EventRule(
            name=name,
            topic=topic,
            predicate=predicate,
            artifact=artifact,
            **windows,
        )


class ResolvedChannel(BaseModel):
    """A channel rule bound to a topic's Arrow schema — schema-payload-ready."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    type: str
    unit: str | None = None
    source_topic: str
    source_field: str
    rate_hz: float
    paths: dict[str, AccessPath]


def _stem(topic: str) -> str:
    return topic.rsplit("/", 1)[-1]


def _check_renames(renames: dict[str, str], allowed: set[str], label: str) -> None:
    """Reject a rename key that matches none of the rule's field paths."""
    for key in renames:
        if key not in allowed:
            raise StreamConfigError(f"{label}.as", f"rename key '{key}' matches no field path")


def _resolve_channel_rule(
    rule: ChannelRule, struct: pa.StructType, label: str
) -> list[ResolvedChannel]:
    resolved: list[ResolvedChannel] = []
    if rule.fields is not None:
        _check_renames(rule.renames, set(rule.fields), label)
        for field_path in rule.fields:
            ap = resolve_path(struct, field_path, field_label=f"{label}.fields")
            try:
                type_name = classify(ap.pa_type)
            except ValueError as exc:
                raise StreamConfigError(f"{label}.fields", str(exc)) from exc
            name = rule.renames.get(field_path, f"{_stem(rule.topic)}.{field_path}")
            resolved.append(
                ResolvedChannel(
                    name=name,
                    type=type_name,
                    source_topic=rule.topic,
                    source_field=field_path,
                    rate_hz=rule.rate_hz,
                    paths={"value": ap},
                )
            )
    else:
        _check_renames(rule.renames, {"geo"}, label)
        paths: dict[str, AccessPath] = {}
        for key, dotted in rule.geo.items():
            ap = resolve_path(struct, dotted, field_label=f"{label}.geo.{key}")
            try:
                if classify(ap.pa_type) != "number":
                    raise ValueError(f"geo '{key}' must resolve to a number, got {ap.pa_type}")
            except ValueError as exc:
                raise StreamConfigError(f"{label}.geo.{key}", str(exc)) from exc
            paths[key] = ap
        name = rule.renames.get("geo", f"{_stem(rule.topic)}.geo")
        resolved.append(
            ResolvedChannel(
                name=name,
                type="geo",
                source_topic=rule.topic,
                source_field=",".join(f"{k}={v}" for k, v in sorted(rule.geo.items())),
                rate_hz=rule.rate_hz,
                paths=paths,
            )
        )
    return resolved


class StreamsConfig(BaseModel):
    """The whole `streams:` manifest section, shape-validated."""

    broker: str | None = None
    flush_interval_s: float = 1.0
    channels: list[ChannelRule] = []
    events: list[EventRule] = []

    @staticmethod
    def build(config: dict) -> "StreamsConfig":
        """Build and validate streams config from dict."""
        if not isinstance(config, dict):
            raise StreamConfigError("streams", f"must be a mapping: {config!r}")
        broker = config.get("broker")
        if broker is not None:
            parsed = urlparse(str(broker))
            if parsed.scheme not in ("mqtt", "mqtts") or not parsed.hostname:
                raise StreamConfigError(
                    "streams.broker",
                    f"must be an mqtt:// or mqtts:// URL with a host: {broker!r}",
                )
        flush = config.get("flush_interval_s", 1.0)
        if not isinstance(flush, int | float) or isinstance(flush, bool) or flush <= 0:
            raise StreamConfigError("streams.flush_interval_s", f"must be > 0: {flush!r}")
        raw_channels = config.get("channels", [])
        if raw_channels is None:
            raw_channels = []
        elif not isinstance(raw_channels, list):
            raise StreamConfigError("streams.channels", f"must be a list: {raw_channels!r}")
        channels = [
            ChannelRule.build(c, label=f"channels[{i}]") for i, c in enumerate(raw_channels)
        ]
        raw_events = config.get("events", [])
        if raw_events is None:
            raw_events = []
        elif not isinstance(raw_events, list):
            raise StreamConfigError("streams.events", f"must be a list: {raw_events!r}")
        events = [EventRule.build(e, label=f"events[{i}]") for i, e in enumerate(raw_events)]
        return StreamsConfig(
            broker=broker,
            flush_interval_s=float(flush),
            channels=channels,
            events=events,
        )

    def resolve(self, structs: dict[str, pa.StructType]) -> list[ResolvedChannel]:
        """Resolve channel rules against topic schemas."""
        out: list[ResolvedChannel] = []
        for i, rule in enumerate(self.channels):
            label = f"channels[{i}]"
            if rule.topic not in structs:
                raise StreamConfigError(f"{label}.topic", f"unknown topic '{rule.topic}'")
            out.extend(_resolve_channel_rule(rule, structs[rule.topic], label))
        seen: set[str] = set()
        for channel in out:
            if channel.name in seen:
                raise StreamConfigError("channels", f"duplicate channel name '{channel.name}'")
            seen.add(channel.name)
        return out


def load_streams(manifest: dict) -> StreamsConfig | None:
    """Parse the `streams:` section of the startup manifest, if present."""
    if not isinstance(manifest, dict) or "streams" not in manifest:
        return None
    return StreamsConfig.build(manifest["streams"])
