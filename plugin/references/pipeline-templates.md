# Pipeline templates

Two starting points for the `authoring-pipelines` skill. Substitute the event
topic, predicate, window and data source. The `module` paths below are for a
ROS 2 `.db3` source; take the right path for the user's format, and its
argument names, from `list_pipeline_capabilities`.

`site`, `asset` and `path` may be Jinja placeholders (for example
`path: "{{ path }}"`), so one saved pipeline can be reused across many logs.

## reduce: one bag with only the event windows

Everything outside the windows is discarded. The reduce task processes the
whole source once, at the end.

```yaml
name: hard_decel_reduce
site: warehouse
asset: forklift
path: ./data/logs/flight_042
allow_failure: false
cadence:
  topic: /imu
  when: once_at_end
tasks:
  - module: bagel_mcp.pipeline.tasks.reduce.ros2.db3
    args:
      event_topic: /imu
      predicate: "imu['linear_acceleration']['x'] < -10"
      pre_seconds: 10
      post_seconds: 10
      debounce_seconds: 2
```

## snippet: one clip per event

The cadence fires once on each rising edge of the predicate. `lookback` sets
the time before each event; `post_seconds` the time after.

```yaml
name: hard_decel_snippets
site: warehouse
asset: forklift
path: ./data/logs/flight_042
allow_failure: false
cadence:
  topic: /imu
  when:
    on_event:
      predicate: "imu['linear_acceleration']['x'] < -10"
      debounce: {last: 2, unit: second}
tasks:
  - module: bagel_mcp.pipeline.tasks.snippet.ros2.db3
    lookback: {last: 10, unit: second}
    args: {post_seconds: 10}
```
