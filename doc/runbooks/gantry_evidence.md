# Gantry Bench evidence bundles

Bagel reads an unzipped Gantry Bench evidence directory containing `manifest.json`
with `magic: GANTRY_EVIDENCE`, plus the CSV files declared in its `tables` object.
This adapter requires only the core Python dependencies; no ROS runtime is needed.

Start with `describe_data_source` on the bundle directory, then `describe_topic`
before querying a table. The `describe/gantry_verdict` capability guides an audit
of the recorded verdict against the evidence. Missing tables mean that evidence
is unavailable; the adapter does not run Gantry's training or robot evaluations.

Table files must be relative paths within the bundle. Absolute paths and symlinks
that resolve outside the bundle are rejected. Declared row counts and column maps
are validated; supported column types are `string`, `int`, `float`, `bool`,
`timestamp`, and `json`. Timestamp and JSON cells remain strings in SQL payloads.

Message timestamps use event `ts`, gate `started_at`, or (for static evidence)
submission `created_at`, falling back to manifest `generated_at` and then epoch
zero when neither is valid. Timestamps without an explicit offset use UTC.
Static evidence rows share one timestamp; they are not a sampled time series.

CSV tables and selected message rows are loaded into memory. This adapter is
intended for evidence reports, not streaming or arbitrarily large recordings.

Run its tests on the host:

```sh
uv sync --locked
uv run pytest test/source/gantry test/message/gantry test/topic/gantry test/test_ci_reachability.py
```

Tests use a generated miniature bundle, including an end-to-end call through
Bagel's description and DuckDB query functions. They do not validate a live
Gantry service or a production training run.
