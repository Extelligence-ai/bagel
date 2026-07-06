# Chat with your InfluxDB 3 data

Point Bagel at an InfluxDB 3 database (Core, Enterprise, or Cloud Dedicated) and every
measurement becomes a queryable topic. Queries go over Arrow Flight SQL and return
Arrow tables natively -- a direct fit for Bagel's DuckDB core, with no serialization
in between.

> InfluxDB 1.x/2.x are in maintenance upstream and are not supported; the Docker
> `influxdb:latest` tag points to InfluxDB 3 Core from September 2026.

## Connect

The data source path is a URL:

```
influxdb://<token>@<host>:<port>/<database>
```

The port defaults to 8181 (InfluxDB 3 Core). The token can instead be passed via
`args` (`{"token": "..."}`), which takes precedence over the URL.

> Describe the data source `influxdb://my-token@influx.local:8181/telemetry`.

- Each measurement (table) is a topic; InfluxDB's mandatory `time` column provides
  timestamps.
- Schemas come from the Arrow results directly, so types are always exact.

## Ask questions

> What's the max temperature in `readings` today?

> Which devices reported humidity above 80% in the last 6 hours?

Queries hit the database on every call -- results are always fresh.

## Find events and extract windows

> Preview keeping 60s before and after every reading above 30°C in `readings`.

Then run a pipeline (`write_topics_to_file`, the S3 upload task, ...) to extract the
windows -- event-windowed reduction over your time series.

## Local test database

```bash
docker run -d --name influx -p 8181:8181 influxdb:3-core \
  influxdb3 serve --node-id node0 --object-store memory --without-auth
```

## Notes and limits (v1)

- InfluxDB 3 **Core** limits query time ranges to roughly the last 72 hours; use
  Enterprise (free for home use) for longer historical windows, and prefer
  time-bounded queries either way.
- Read-only; writes are out of scope.
