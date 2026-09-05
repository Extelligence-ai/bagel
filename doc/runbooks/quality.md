# Quality contracts

Pipeline source interpretation belongs in `source_args`, for example
`source_args: {timestamp_column: t, timestamp_format: seconds}`. Cadence, gates,
and tasks use the same adapters and timestamp options. Existing operator `setup`
options are promoted to shared options; conflicting interpretations fail before
operators are constructed. Cadence intervals must be positive integers; lookbacks
must be nonnegative; event debounce/forward windows require time units.

Live `once_at_end` runs after subscription shutdown, once, and only if messages
arrived. Frequency cadence retains its first-message behavior.

`run_pipeline` returns `status: completed`, `partial`, or `failed` and a `runs`
object with `succeeded`, `skipped`, `failed`, and `errors`. These count cadence runs,
not individual tasks. `allow_failure` controls continuation, not whether a failed
run is reported. Batch results also expose partial sources. Artifacts produced
before a failure remain reported. Callers must handle the new `partial` status.

Previews and reduction share source-bound clipping. `kept_seconds` measures the
union of windows that intersect the source's timestamp extent, not requested
time outside the recording. Empty or zero-duration sources report fraction zero.
The legacy standalone `plan_reduction` helper still accepts a duration without
bounds; tool and reduce paths always supply explicit bounds.

CSV/JSON readers scan bounded Arrow batches and use DuckDB's external sort to
preserve timestamp order (including stable ties). Cadence/event scans consume
bounded result batches. Memory still scales with requested output: a tool that
returns every row or every detected event necessarily retains that result.

Run the portable suite with isolated test storage through `test/conftest.py`.
Cache regressions deliberately issue multiple requests within one test. DuckDB
relations belong to their creating thread; materialize a result before passing it
between threads. Use `relation.query(name, sql)` instead of registering a Bagel
relation on DuckDB's global connection.
