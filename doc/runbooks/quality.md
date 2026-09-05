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

## CI enforcement and rollout

`test.yaml` runs all eleven service images and host tests on Python 3.10, 3.11,
and 3.12 using the lockfile. Host jobs install IoT, automotive, upload, and
visualization groups, require optional suites to collect, and reject unexpected
skips. Only ROS2 runtime tests and the four live-database tests may skip there;
the database job supplies disposable PostgreSQL and InfluxDB and permits no skips.
Adversarial/e2e markers apply only to their packages. Tests time out after five
minutes and unhandled thread exceptions fail tests. The known asammdf destructor
warning remains visible rather than promoting all third-party warnings to errors.

Coverage includes `src`, `server`, and `run`, with branch measurement enabled.
Every expected matrix artifact must exist before combination. Separate floors
retain 64% line coverage and introduce 50% branch coverage. Raise these as tests
improve; do not lower them to make unrelated changes pass. Formatting, workflow
syntax, and strict incremental typing of query/source-context/run-results helpers
are required. This is an incremental typing boundary, not a whole-repo type claim.

Production images share `docker/sync-deps.sh`, install locked non-dev dependencies,
and set `UV_NO_SYNC=1` so startup cannot reinstall dev tools. Build smoke checks
import the server and assert pytest/Jupyter are absent. Publication re-runs tests,
lint and dependency checks for the exact main commit before any image push.

The audit exports all locked groups for the Linux/Python 3.12 runner and blocks
new package/advisory pairs relative to the PR base or previous main commit.
Existing advisories are grandfathered, **not declared safe or triaged**; complete
JSON reports remain available in weekly/PR runs. Audit errors and skipped packages
fail the gate. Weekly same-commit audits report existing findings without blocking
on pre-existing debt. Platform-specific dependencies outside that runner remain
outside this audit's scope. Action revisions are immutable and Dependabot proposes
weekly updates. The auditor uses the documented
[pip-audit requirements mode](https://github.com/pypa/pip-audit); disposable Influx
seeding uses its [v3 write API](https://docs.influxdata.com/influxdb3/core/api/write-data/).

### Required repository settings (maintainer action)

After the landing PR demonstrates green checks, retain `ruff`, `hadolint`, and
`codex-review-gate` and add `quality-gate`, `types-and-workflows`, and `pip-audit`
as required checks in the main ruleset. Confirm the exact displayed check names
from the landing run before saving. Do not require each matrix entry individually:
`quality-gate` aggregates all service, host, coverage and database results. These
PRs do not mutate branch-protection settings or merge themselves.

### External recordings

Hosted scheduled runs always exercise deterministic memory/adversarial/sample
regressions. Real-recording coverage requires a dedicated self-hosted runner
labeled `bagel-fixtures`, repository variable `BAGEL_EXTERNAL_RUNNER=enabled`,
`BAGEL_EXTERNAL_FIXTURES` pointing at the MCAP recordings directory (including
the gps-loss recording), and `BAGEL_LARGE_FIXTURE` pointing at a real large capture
(CAN also requires its sibling DBC). Install any format-specific runtime on that
runner. The external job runs only trusted main code, permits no skips, and has
a 45-minute limit. Until configured, the schedule explicitly reports that this
coverage is **not enabled**. Fixture provisioning is a deployment prerequisite,
not something a passing hosted run proves.
