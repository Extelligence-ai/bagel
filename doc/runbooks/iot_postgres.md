# Chat with your TimescaleDB / PostgreSQL data

Point Bagel at a standard connection URL and every table becomes a queryable topic --
describe, DuckDB SQL, event previews, and event-windowed extraction all work directly
against the database (read-only, via DuckDB's `postgres` extension; no separate driver).
TimescaleDB hypertables are plain tables from Bagel's point of view.

## Connect

Use the URL as the data source path anywhere a file path is accepted:

> Describe the data source `postgres://user:pass@db.local:5432/telemetry`.

- Each user table is a topic (`readings`, or `myschema.readings` outside `public`).
- The timestamp column is detected per table (preferring names like `time`,
  `timestamp`, `ts`, `created_at`, then any TIMESTAMP-typed column) and converted to
  epoch seconds. Override per table via `args`:
  `{"timestamp_columns": {"readings": "logged_at"}}`.

## Ask questions

> What's the max temperature in `readings` over the last day?

> Correlate `temp` and `humidity` in `readings` -- anything unusual?

Queries hit the database directly on every call, so results are always fresh (no
Arrow-file caching).

## Find events and extract windows

The event tooling works unchanged:

> Preview keeping 60s before and after every reading above 30°C in `readings`.

which calls `preview_pipeline(path="postgres://...", event_topic="readings",
predicate="\"readings\"['temp'] > 30", ...)` and reports the events and kept fraction --
then a pipeline with `write_topics_to_file` (or the S3 upload task) extracts the windows.

## Local test database

```bash
docker run -d --name pg -p 5432:5432 -e POSTGRES_PASSWORD=secret postgres:16-alpine
# or timescale/timescaledb:latest-pg16
```

## Notes and limits (v1)

- Read-only access; the DuckDB `postgres` extension is pre-installed in the `iot` image.
- Tables without any TIMESTAMP-typed column need an explicit `timestamp_columns` entry.
- Very large tables: prefer SQL aggregations and time windows (`start_seconds` /
  `end_seconds`) -- full-table scans go over the wire.
