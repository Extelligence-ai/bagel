"""Standing pipelines: subscribe to live topics with attached pipelines, at boot.

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
"""

import logging
import pathlib
from typing import Any

import yaml

from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.topic_sink import TopicSink, guess_host, guess_port
from src.pipeline import base


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
        One report per subscription entry: `{"sink", "status", "topics" | "error"}`.

    """
    manifest = yaml.safe_load(pathlib.Path(manifest_file).read_text()) or {}
    reports: list[dict[str, Any]] = []

    for entry in manifest.get("subscriptions", []):
        sink_type = TopicSink(entry["sink"])
        try:
            sink = module.provide(
                f"{BaseModule.TOPIC_SINK.value}.{sink_type.value}",
                {
                    "host": entry.get("host") or guess_host(sink_type),
                    "port": entry.get("port") or guess_port(sink_type),
                    **(entry.get("args") or {}),
                },
            )
            topics = subscribe_with_pipeline(
                sink, entry.get("topics"), entry.get("pipeline")
            )
            reports.append(
                {"sink": sink_type.value, "status": "subscribed", "topics": topics}
            )
            logging.info(
                "Startup subscription active: %s -> %s", sink_type.value, topics
            )
        except Exception as error:
            logging.error(
                "Startup subscription failed for sink '%s': %s", sink_type.value, error
            )
            reports.append(
                {"sink": sink_type.value, "status": "failed", "error": str(error)}
            )
    return reports
