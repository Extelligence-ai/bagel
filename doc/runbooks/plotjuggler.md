# Bagel + PlotJuggler

[PlotJuggler](https://plotjuggler.com/) is where you *look* at signals; Bagel is where
you *ask questions* and decide what data to keep. They compose naturally: chat with
Bagel to find and cut the interesting windows, then eyeball them in PlotJuggler.

## Open Bagel's outputs directly (no export needed)

Every reduced bag and event snippet Bagel writes is a standard `.mcap` file —
PlotJuggler (≥ 3.6) opens them natively, any profile:

> Find every hard brake in this bag and cut ±10s clips around each.

…then drag the clips from `~/.bagel/artifacts/` into PlotJuggler.

## Flattened exports for the CSV/Parquet loaders

Bagel's default CSV/Parquet exports keep each topic as a struct column (great for SQL,
opaque to plotting tools). Pass `flatten: true` to get **one scalar column per
signal**, named the way PlotJuggler users expect (`/imu/linear_acceleration/x`):

```yaml
tasks:
  - module: src.pipeline.tasks.write_topics_to_file
    args: {topics: ["/imu"], output_format: csv, flatten: true}
    lookback: {last: 10, unit: second}
```

Or conversationally:

> Export the 30 seconds around that anomaly as a flattened CSV for PlotJuggler.

Non-scalar fields (lists, maps) are skipped; the `timestamp_seconds` column comes
first, ready for PlotJuggler's "use column as X axis" prompt.

## Watching live data side by side

PlotJuggler's MQTT/ROS streaming plugins can subscribe to the **same broker or bridge**
Bagel is watching — live curves in PlotJuggler while Bagel runs event detection and
standing pipelines on the identical stream. No integration needed; point both at the
same endpoint.

## Which tool when?

| You want to… | Use |
| --- | --- |
| Eyeball signals, zoom, compare curves | PlotJuggler |
| Ask questions in plain language ("did it overheat?") | Bagel |
| Find events and keep only the data around them | Bagel |
| Inspect the reduced/snippet output | PlotJuggler (opens the `.mcap` directly) |
