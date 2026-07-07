# Chat with your IoT data over MQTT

Bagel subscribes to any MQTT 3.1.1/5.0 broker (Mosquitto, EMQX, HiveMQ, AWS IoT Core, ...)
and buffers topic messages locally, where every Bagel tool works on them: describe, SQL
queries, previews, and live event-driven pipelines. Pure Python -- no ROS required.

## Start Bagel

```bash
docker compose run --service-ports iot
```

Then connect your MCP client as usual (see the README quickstart).

## Discover and subscribe

MQTT brokers have no topic-listing API, so Bagel listens on the `#` wildcard for a short
window (`discovery_seconds`, default 2s) and reports the topics it sees -- retained
messages show up immediately. From your MCP client:

> List the available live topics on the MQTT broker at `broker.local`.

> Subscribe to `freezer/1/status` and `freezer/2/status` from the MQTT broker.

Subscribed messages are buffered to a local sink directory; ask questions about them at
any time:

> What's the average temperature on `freezer/1/status` over the buffered data?

## Payloads and schemas

Payloads are parsed as JSON (the ecosystem norm): JSON objects become queryable structs
(schema inferred from sampled messages), bare scalars become `{"value": ...}`, and
non-JSON text becomes `{"payload": "<text>"}`. A sample payload doubles as the topic's
"definition" when you ask Bagel to describe it. Sparkplug B / protobuf payloads are a
planned extension.

Authentication and transport: pass `username`/`password`, `tls: true` (with optional
`tls_ca_certs`), and `transport: "websockets"` via the `args` of `subscribe_live_topics`
(they are forwarded to the sink constructor). Timestamps default to message arrival
time; if your payloads carry one, set `timestamp_field` (and `timestamp_unit`:
second/millisecond/microsecond/nanosecond) to use it instead.

## Live edge-recording

Attach an event-driven pipeline so only the moments that matter are acted on -- for
example, alert on a cold-chain excursion:

```yaml
name: freezer_excursion
site: warehouse
asset: freezer_1
path: <sink directory>            # printed by subscribe_live_topics
allow_failure: true
cadence:
  topic: freezer/1/status
  when:
    on_event:
      predicate: "\"freezer/1/status\"['temp'] > -15"
      debounce: {last: 120, unit: second}
      forward: {last: 30, unit: second}   # capture 30s after the excursion begins
tasks:
  - module: src.pipeline.tasks.write_topics_to_file
    lookback: {last: 30, unit: second}
    args: {topics: ["freezer/1/status"], output_format: csv}
```

The `on_event` cadence fires once per excursion (debounced), the `forward` window delays
firing until the post-event data has been buffered, and the task snapshots the window --
swap in `send_email` or the S3 upload task as needed.

## Local test broker

```bash
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2 \
  mosquitto -c /mosquitto-no-auth.conf
# publish something
docker exec mosquitto mosquitto_pub -t 'freezer/1/status' \
  -m '{"temp": -18.5, "door": "closed"}' -r
```

## Notes and limits (v1)

- Subscribe to exact topic names; wildcards are used internally for discovery only.
- Schema is inferred from early samples; fields that appear later read as NULL.
- Messages carry no standard timestamp, so arrival time is used.
