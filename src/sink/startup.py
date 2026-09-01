r"""Standing pipelines: subscribe to live topics with attached pipelines, at boot.

Two entry points share the same logic:

- The ``subscribe_live_topics`` MCP tool accepts a ``pipeline`` config, so a standing
  edge pipeline can be created conversationally.
- A startup manifest (``settings.STARTUP_PIPELINES_FILE``) declares subscriptions and
  their pipelines; the server applies it on boot, so pipelines survive restarts when
  the container runs under a restart policy.

Manifest format::

    subscriptions:
      - sink: mqtt                    # TopicSink type (mqtt, ros2.bridge, ...)
        host: broker.local            # optional; defaults are guessed per sink type
        port: 1883                    # optional
        args: {username: bagel}       # optional extra sink constructor arguments
        topics: ["freezer/1/status"]
        pipeline:                     # optional; attaches to its cadence.topic
          name: freezer_excursion
          site: warehouse
          asset: freezer_1
          allow_failure: true
          cadence: {topic: "freezer/1/status", when: {on_event: {...}}}
          tasks: [{module: src.pipeline.tasks.write_topics_to_file, ...}]

The pipeline's ``path`` defaults to the sink directory, so tasks read the live buffer.

A manifest may also carry a top-level ``streams:`` section (fleet streaming,
spec §2-§6; shape validated by ``src.sink.publish.config.load_streams``)::

    streams:
      broker: mqtts://fleet.example.com:8883   # optional; defaults to the
                                                # enrolled identity's broker_url
      flush_interval_s: 1.0                    # optional; default 1.0
      channels:
        - topic: "robot/telemetry"
          fields: ["speed", "battery.percent"]
          rate_hz: 5
      events:
        - name: low_battery
          topic: "robot/telemetry"
          predicate: "\"robot/telemetry\"['battery']['percent'] < 10"

After the ``subscriptions:`` loop above runs, ``start()`` starts fleet
streaming from this section, if present, gated on ``settings.FLEET_ENABLED``
and on a viable broker (an enrolled fleet identity, or an explicit
``mqtt://`` ``broker:`` eligible under ``settings.FLEET_DEV_INSECURE`` -- see
``src/sink/publish/connect.py``). RULING A (v1 limitation, binding): every
topic referenced by ``streams.channels``/``streams.events`` must be
subscribed within a SINGLE ``subscriptions:`` entry -- ``FleetService``
taps one sink's topic buffers, and each entry's sink is a fresh instance
(``module.provide`` never shares one across entries), so a ``streams:``
block whose topics span two different subscription entries cannot be
served; it produces a failed fleet report entry naming the limitation
instead. RULING B (v1, binding): there is no ``TopicSink.close()`` ->
``FleetService.stop()`` coupling -- the fleet service, once started, runs
for the process's lifetime (daemon threads, a publisher LWT, and its
finalizer cover process exit; see ``src/sink/publish/service.py``).

The result is one additional report entry, ``{"fleet": "started" | "disabled"
| "failed", ...}``, alongside the per-subscription ones -- or none at all
when the manifest has no ``streams:`` section.
"""

import logging
import pathlib
from typing import Any

import yaml

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.topic_sink import TopicSink, guess_host, guess_port
from src.pipeline import base
from src.sink.publish import StreamConfigError
from src.sink.publish import identity as identity_mod
from src.sink.publish.config import StreamsConfig, load_streams
from src.sink.publish.connect import resolve_publisher_kwargs
from src.sink.publish.mqtt import MqttPublisher
from src.sink.publish.service import FleetService
from src.sink.publish.spool import Spool

# Step 7's fleet status/control tools read and (for tests) stop this; None
# until/unless a manifest's `streams:` section successfully starts one.
_FLEET_SERVICE: FleetService | None = None


def subscribe_with_pipeline(
    sink: object,
    topics: list[str] | None,
    pipeline_config: dict[str, Any] | None,
    overwrite: bool = False,
) -> list[str]:
    """Subscribe to topics on a sink, attaching a pipeline to its cadence topic.

    Args:
        sink: The TopicSink to subscribe on.
        topics: Topics to subscribe to. If None, all available topics.
        pipeline_config: A pipeline configuration (same structure `run_pipeline`
            accepts). Its `path` defaults to the sink directory so tasks read the live
            buffer, and its `cadence.topic` must be among the subscribed topics -- the
            pipeline runs on that topic's incoming messages for the life of the
            subscription. If None, topics are subscribed without a pipeline.
        overwrite (bool, optional): Passed through to `subscribe`. Defaults to False.

    Returns:
        The list of subscribed topics.

    Raises:
        ValueError: If the pipeline's cadence topic is not among the subscribed topics.

    """
    topics = topics or sink.available_topics

    pipeline = None
    pipeline_topic = None
    if pipeline_config is not None:
        pipeline_config = {"path": str(sink.directory), **pipeline_config}
        pipeline = base.Pipeline.build(pipeline_config)
        pipeline_topic = pipeline.cadence.topic
        if pipeline_topic not in topics:
            raise ValueError(
                f"Pipeline cadence topic '{pipeline_topic}' is not among the "
                f"subscribed topics: {topics}"
            )

    # All-or-nothing admission: refuse the whole batch up front rather than
    # subscribing a prefix and failing mid-loop (Codex review on #156).
    sink.ensure_capacity(list(topics), overwrite=overwrite)

    for topic in topics:
        sink.subscribe(
            topic,
            pipeline=pipeline if topic == pipeline_topic else None,
            overwrite=overwrite,
        )
        if topic == pipeline_topic:
            logging.info(
                "Standing pipeline '%s' attached to live topic '%s'",
                pipeline.name,
                topic,
            )
    return list(topics)


def start(manifest_file: str | pathlib.Path) -> list[dict[str, Any]]:
    """Apply a startup manifest: connect sinks, subscribe topics, attach pipelines.

    Each subscription is applied independently -- a broker that is down at boot logs an
    error and does not prevent the others (or the server) from starting.

    Args:
        manifest_file: Path to the YAML manifest (see module docstring for the format).

    Returns:
        One report per subscription entry: `{"sink", "status", "topics" | "error"}`,
        plus (when the manifest has a `streams:` section) one trailing fleet
        report entry: `{"fleet": "started" | "disabled" | "failed", ...}` --
        see the module docstring.

    """
    try:
        manifest = yaml.safe_load(pathlib.Path(manifest_file).read_text()) or {}
    except (OSError, yaml.YAMLError) as error:
        logging.error("Failed to read startup manifest '%s': %s", manifest_file, error)
        return []
    if not isinstance(manifest, dict):
        logging.error(
            "Startup manifest '%s' must be a mapping, got %s",
            manifest_file,
            type(manifest).__name__,
        )
        return []

    reports: list[dict[str, Any]] = []
    subscribed: list[tuple[object, list[str]]] = []
    for entry in manifest.get("subscriptions", []):
        try:
            sink_type = TopicSink(entry["sink"])
        except Exception as error:
            logging.error(
                "Startup subscription failed for sink '%s': %s",
                entry.get("sink", "<missing>"),
                error,
            )
            reports.append(
                {
                    "sink": entry.get("sink", "<missing>"),
                    "status": "failed",
                    "error": str(error),
                }
            )
            continue
        try:
            sink = module.provide(
                f"{BaseModule.TOPIC_SINK.value}.{sink_type.value}",
                {
                    "host": entry.get("host") or guess_host(sink_type),
                    "port": entry.get("port") or guess_port(sink_type),
                    **(entry.get("args") or {}),
                },
            )
            topics = subscribe_with_pipeline(sink, entry.get("topics"), entry.get("pipeline"))
            subscribed.append((sink, topics))
            reports.append({"sink": sink_type.value, "status": "subscribed", "topics": topics})
            logging.info("Startup subscription active: %s -> %s", sink_type.value, topics)
        except Exception as error:
            logging.error("Startup subscription failed for sink '%s': %s", sink_type.value, error)
            reports.append({"sink": sink_type.value, "status": "failed", "error": str(error)})

    fleet_report = _start_fleet(manifest, subscribed)
    if fleet_report is not None:
        reports.append(fleet_report)
    return reports


def _fleet_source_topics(streams: StreamsConfig) -> set[str]:
    """All topics `streams.channels`/`streams.events` reference (RULING A's coverage set)."""
    return {rule.topic for rule in streams.channels} | {rule.topic for rule in streams.events}


def _find_covering_sink(
    source_topics: set[str], subscribed: list[tuple[object, list[str]]]
) -> object | None:
    """Return the first subscribed sink whose topics cover every `source_topic`.

    RULING A (v1 limitation, binding -- see module docstring): fleet
    streaming taps a single sink's topic buffers, and each `subscriptions:`
    entry gets its own fresh sink instance, so a `streams:` block whose
    source topics span two different entries can never be served. `None`
    (never raised) signals "no single entry covers them all" -- for the
    caller to turn into one actionable report entry, whether that is because
    no entry subscribes to any of them, only some of them, or they are split
    across more than one entry's topics.
    """
    for sink, topics in subscribed:
        if source_topics <= set(topics):
            return sink
    return None


def _start_fleet(
    manifest: dict[str, Any], subscribed: list[tuple[object, list[str]]]
) -> dict[str, Any] | None:
    """Start fleet streaming from the manifest's `streams:` section, if present.

    Mirrors the per-subscription isolation above: every failure here --
    manifest shape, `FLEET_ENABLED`, topic coverage (RULING A), missing
    identity, a broker that fails `resolve_publisher_kwargs`'s dev-insecure
    check, or anything `FleetService.start()` raises -- is caught and turned
    into `{"fleet": "failed", "error": str(exc)}` rather than raised, so a
    fleet misconfiguration never prevents the server (or the rest of the
    manifest's subscriptions) from starting.

    Returns:
        `None` when the manifest has no `streams:` section at all (no report
        entry is added in that case); otherwise the fleet report entry.

    """
    global _FLEET_SERVICE  # noqa: PLW0603 -- the module-level holder step 7's tools read/stop
    try:
        streams = load_streams(manifest)
        if streams is None:
            return None
        if not settings.FLEET_ENABLED:
            logging.info(
                "Startup manifest declares 'streams:' but fleet streaming is disabled "
                "(FLEET_ENABLED=0); ignoring it"
            )
            return {"fleet": "disabled"}

        source_topics = _fleet_source_topics(streams)
        sink = _find_covering_sink(source_topics, subscribed)
        if sink is None:
            raise StreamConfigError(
                "streams",
                "all streams: source topics "
                f"{sorted(source_topics)} must be subscribed within a SINGLE "
                "startup manifest 'subscriptions:' entry -- fleet streaming "
                "(v1) cannot span multiple subscription entries' sinks; list "
                "every one of these topics under one entry's 'topics:'",
            )

        if _FLEET_SERVICE is not None:
            # A second startup.start() in the same process is about to
            # replace the holder: FleetService.start()'s double-start guard
            # is per-instance, so nothing else would ever stop the PREVIOUS
            # service's daemon threads, MQTT connection, or tap wiring once
            # this reassigns it -- stop it first, before the new one is even
            # built, so its taps are cleared before the new service (which
            # may tap the very same sink/buffers) wires its own. Best-effort:
            # a broken old service must not block the new one from starting.
            try:
                _FLEET_SERVICE.stop()
            except Exception as stop_error:
                logging.warning(
                    "Failed to stop the previous FleetService before replacing it: %s",
                    stop_error,
                )
            # Clear the holder immediately, whether or not stop() itself
            # succeeded -- if the NEW service below fails to start, the
            # holder must not keep pointing at this now-stopped instance as
            # if it were still the live one (a caller reading it -- step 7's
            # status/control tools -- would see a dead service as live).
            _FLEET_SERVICE = None

        directory = settings.FLEET_IDENTITY_DIRECTORY
        identity = (
            identity_mod.load_identity(directory) if identity_mod.is_enrolled(directory) else None
        )
        publisher_kwargs = resolve_publisher_kwargs(streams, identity)
        publisher = MqttPublisher(**publisher_kwargs)
        spool = Spool.for_robot(identity.robot if identity is not None else "dev/robot")
        service = FleetService(
            sink=sink, streams=streams, publisher=publisher, spool=spool, identity=identity
        )
        service.start()
        _FLEET_SERVICE = service
        logging.info(
            "Fleet streaming started: tenant=%s robot=%s",
            identity.tenant if identity is not None else "dev",
            identity.robot_id if identity is not None else "robot",
        )
        return {"fleet": "started"}
    except Exception as error:
        logging.error("Fleet streaming failed to start: %s", error)
        return {"fleet": "failed", "error": str(error)}
