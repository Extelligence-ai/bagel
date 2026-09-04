# Stream live telemetry to a fleet service

Bagel can stream selected channels, events and heartbeats from a robot to a fleet
broker over MQTT/mTLS, with store-and-forward on disk so an unreliable link never
loses data. The wire contract is pinned in [`doc/fleet_protocol_v1.md`](../fleet_protocol_v1.md);
this runbook is the operator's side: enroll, stream, pause, verify, unenroll.

## Enroll a robot

Two paths, same result -- an identity lands in `FLEET_IDENTITY_DIRECTORY`
(default `~/.bagel/identity`): `robot.key` (0600, generated on the robot and
never transmitted), `robot.crt`, `ca.crt`, and `identity.yaml` (tenant, robot
id, broker URL, expiry). Renewals later add versioned file names; `identity.yaml`
always points at the current set.

**First boot, hands-off:** set both env vars before starting the container --

```bash
FLEET_ENROLL_TOKEN=<one-time token>
FLEET_ENROLL_URL=https://fleet.example.com
```

The server enrolls itself at boot if (and only if) it isn't already enrolled.
Once `identity.yaml` exists, both variables are ignored and can be removed.

**Interactively:** from your MCP client,

> Enroll this robot with the fleet broker at `https://fleet.example.com` using token `<token>`.

(`enroll_fleet_identity`). An already-enrolled robot refuses rather than
overwriting -- unenroll first. Enrolling while a dev-placeholder streaming
service is already running leaves that service unchanged; the new identity
only takes effect on the next `stream_live_topics`/`stop_live_streams` restart.

### Token pitfalls

Enrollment tokens are single-use. If the robot crashes between the server's
HTTP 200 and storing the identity locally, the token is consumed but the robot
has nothing to show for it -- a retry gets `410`. Re-issue a fresh token and
retry; there is nothing to clean up on the robot. The token is never logged
and never stored, so there is no copy to recover (or leak) after the attempt.

## Kill switch

`FLEET_ENABLED=0` makes the fleet subsystem fully inert: nothing connects,
nothing leaves the box, the streaming tools refuse, and **first-boot
enrollment is skipped too** -- an env-configured token is left unconsumed for
when you re-enable. Two tools still work regardless: `describe_stream_status`
(so you can see *that* it's disabled) and `unenroll_fleet_identity` (which
only makes things more inert).

Don't confuse it with `BAGEL_FLEET`: that is a **build-time** Docker
argument (it decides whether the fleet dependency group is installed into
the image at all) and has **no runtime effect**. Setting it as a runtime
environment variable does nothing except log this warning at startup:

```
BAGEL_FLEET is a build-time image argument and has no runtime effect; to disable fleet streaming set FLEET_ENABLED=0
```

The runtime kill switch is `FLEET_ENABLED`, nothing else.

## Certificate renewal

Renewal is automatic: from 30 days before the certificate expires, the robot
attempts a renewal once per day (rate limit persists across restarts) and
carries on with its current certificate on any failure. While a fleet service
ships renewal disabled, the robot's daily attempt gets `404`/`501` and treats
it as expected -- no operator action, nothing in the logs above INFO.

The renew endpoint can differ from the enroll endpoint: the enroll response's
optional `renew_url` tells the robot where to renew, and each renew response
may rotate it again. The client honors both as of this release -- but until
the fleet service starts sending `renew_url` in renew responses, rotating the
renewal endpoint of an already-enrolled robot requires unenroll + re-enroll.

## Operate

`describe_stream_status` is the local source of truth -- it answers sanely in
every state (not installed, disabled, unenrolled, stopped, paused, running)
and never depends on the broker being reachable:

> Is this robot streaming to the fleet broker right now?

Add or replace streaming rules with `stream_live_topics`, remove them with
`stop_live_streams`:

> Stream battery percent from `robot/telemetry` at 1 Hz to the fleet broker.

Rule changes restart the streaming service: expect a brief reconnect blip
(the retained schema is republished, and the disk spool preserves anything
queued across the gap). Rule changes preserve paused state; the service
reconnects briefly to republish the schema, then re-pauses -- it never
silently comes back online. Rules applied this way are persisted to the
startup manifest's `streams:` section, so they survive container restarts.

Event rules are accepted, validated, merged, and persisted the same way
channel rules are (`stream_live_topics`/`stop_live_streams` report the live
rule names back as `events`) -- and they are evaluated on-robot: a rule
reported back is live, not merely configured. See "Events" below.

`pause_fleet_streaming` / `resume_fleet_streaming` take the connection
offline and back without touching identity or rules -- a paused robot leaves
a retained `reason: "paused"` heartbeat on the broker, so fleet-side
dashboards can tell it from a crash. `pause_fleet_streaming(discard=True)`
also drops the unsent channels backlog.

Disk budget: the spool's channels lane is capped by `FLEET_SPOOL_MAX_BYTES`
(default 256 MB) -- oldest batches are evicted first under a sustained
disconnect, and the heartbeat's `spool.evicted` counter reports it. Events
and heartbeats are never dropped.

`unenroll_fleet_identity` stops any live streaming, deletes exactly the
identity file set (key, cert, CA cert, `identity.yaml`, plus any versioned
renewal leftovers) and removes the manifest's `streams:` section -- nothing
else in the manifest is touched. Re-enrolling needs a fresh token.

## Events

An event rule names a topic and a predicate, and fires when the predicate
goes from false to true on the live stream:

```yaml
streams:
  events:
    - name: hard_decel
      topic: "robot/telemetry"
      predicate: "\"robot/telemetry\"['decel'] > 8"
      pre_seconds: 5          # window captured before the firing
      post_seconds: 2         # window captured after (delays the firing)
      debounce_seconds: 30    # coalesce edges closer together than this
      artifact: mcap          # capture the window into a robot-local MCAP
```

The predicate is SQL over the topic's own fields, referenced as
`topic['field']` (add more `['field']` steps for nesting). Predicates are
validated when the service starts (and when `stream_live_topics` applies a
rule): a bad one -- broken SQL, an unknown field, an unsubscribed topic --
is rejected up front with a typed error, never silently accepted and never
fired.

**Rate cap.** Each rule fires at most `FLEET_EVENTS_MAX_PER_MINUTE` times
(default 6) in any trailing 60-second window. The configured windows and
debounce are never mutated to enforce this; excess firings are counted and
reported as `summary.suppressed` on the next firing that gets through --
loss is visible, never silent.

**Sample-drop honesty.** The sample path feeding the event engine is
deliberately bounded (an in-memory queue plus per-topic rings capped by
`FLEET_EVENT_RING_MAX_SAMPLES`/`FLEET_EVENT_RING_MAX_BYTES`), so under
extreme load *samples* can drop -- the health report's `events_pipeline`
check and `describe_stream_status` count exactly that. Emitted *events*
never drop: once a firing is recorded it goes to the never-evicted events
spool lane and is delivered.

**Artifacts.** `artifact: mcap` captures the firing's pre/post window into
`CACHE_DIRECTORY/publish-artifacts/{robot}/{name}-{event_id}.mcap`. The
store is capped by `FLEET_ARTIFACTS_MAX_BYTES` (default 256 MB), oldest
files evicted first. The event's `artifact.uri` is a `file://` URI local to
the robot -- v1 has no upload; fetching the file (scp, a collector sidecar,
a shared volume) is the operator's job. An artifact failure (budget
exceeded, write error) never blocks the event: it arrives without
`artifact`, carrying `summary.artifact_error` instead. Keep windowed
(`pre_seconds`/`post_seconds`) rules on low-rate topics -- every sample of a
windowed topic is evaluated and ring-buffered, so a high-rate topic (a
camera, a dense pointcloud) belongs in a recorded bag, not an event window.

**Removing a rule with no service running** updates only the persisted
manifest -- with nothing streaming there are no live topics to re-validate
the remaining rules against. If a stale, now-invalid event rule is left in
the manifest this way, the next start does not crash: the boot report
carries a `fleet: "failed"` entry whose error points at the offending
`events[i]` entry, and streaming stays down until you fix (or remove) the
rule and restart.

## Health reports

The robot publishes a scheduled self-diagnosis (`name: "health_report"`,
`source_topic: "internal:health"`) on the events stream: once shortly after
each streaming session starts (`FLEET_HEALTH_SETTLE_S`, default 60 s -- the
settle delay keeps boot-time transients out of the first report), then every
`FLEET_HEALTH_INTERVAL_S` (default 6 h). There is no on-demand trigger.
Pausing and resuming starts a new session, so a resume restarts the settle
clock and a fresh settle-delayed report follows.

Ten checks, each `pass`/`warn`/`fail`/`skipped` with a `reason` when not
passing:

- `connection` -- fails if the router thread died; warns while
  offline-but-retrying.
- `queue` -- warns while the router's sample queue is actively dropping.
- `events_pipeline` -- warns while the event emitter's queue is actively
  dropping, or on any predicate error on live messages.
- `spool` -- fails if the channels lane evicted (lost) batches this period;
  warns above 80% of `FLEET_SPOOL_MAX_BYTES`.
- `events_backlog` -- warns when more than 1000 events are spooled but not
  yet delivered (events aren't reaching the fleet service).
- `disk` -- fails below 512 MB free on the cache filesystem; warns below
  2 GB.
- `certificate` -- skipped when unenrolled; fails when expired; warns inside
  the 30-day renewal window.
- `topic_staleness` -- warns when a tapped topic has been silent (or never
  seen) for 5+ minutes. Advisory: source timestamps may be sim time, so a
  legitimately idle or replaying robot can trip it.
- `heartbeat` -- fails if the heartbeat thread died; warns on growing
  spool-append failures or a publish error.
- `artifacts` -- skipped with no artifact-bearing rules; warns above 80% of
  `FLEET_ARTIFACTS_MAX_BYTES`.

Counter-based checks are *deltas* against the previous report (the report's
`t_start`/`t_end` span exactly that period): they flag problems actively
occurring since the last report, so a one-time historical blip does not warn
forever.

## Build provenance

Set `BAGEL_BUILD_ID` (and optionally `BAGEL_VCS_REF`) to stamp every
heartbeat and every event summary with a `build` block -- the fleet side can
then correlate behavior with the exact software build. Bake them into the
image at build time (e.g. Docker `ENV BAGEL_BUILD_ID=...` set from your CI's
build metadata). Empty or unset means the block is simply absent;
`BAGEL_VCS_REF` alone does nothing (`build_id` is the required key).

## Selftest

To prove a broker + ingestion pipeline end to end with no robot data in the
loop -- after enrolling, or against a dev broker:

```bash
uv run python -m src.sink.publish.selftest            # enrolled robot
uv run python -m src.sink.publish.selftest --broker mqtt://localhost:1883  # dev rig
```

It publishes a fixed four-channel schema, ten deterministic batches, a
heartbeat and an event (exact expected values are in the contract doc's
Conformance section), prints a one-line JSON summary, and exits 0. It keeps
publishing AS the enrolled robot on that robot's real topics, but none of its
own messages are retained and it arms no last-will, so it leaves no residue
behind once it's done -- a real robot's own retained schema/heartbeat, and
the live service's own last-will, are left completely untouched.

Run it with fleet streaming **paused or stopped**: it shares the robot's real
spool lanes and holds the spool lock for its entire run, so a concurrently
running service's ingestion blocks until the selftest finishes -- sequence
integrity is preserved either way, but on a high-rate robot the paused
ingestion can surface as counted queue drops (`queue.dropped` in the next
heartbeat). Pausing first avoids both the wait and the drops. If another
writer already holds the lock, the selftest refuses with a typed error
rather than waiting indefinitely.

The selftest also refuses outright if either spool lane already has pending
unacked backlog (e.g. a paused service's queued-but-unsent data) -- exit 1,
nothing touched -- since it would otherwise ack its way past that backlog and
silently drop it; let the service drain the backlog, or discard it first via
`pause_fleet_streaming(discard=True)`. Note that the events lane fills on
its own now: event firings and scheduled health reports emitted while the
broker is unreachable sit there as pending backlog, so a robot that has
been offline for a while will make the selftest refuse -- that's not a
fault. Bring the service online, let the backlog drain, then run the
selftest. (Discarding is not an option for that backlog: `discard=True`
only ever empties the channels lane; events are never dropped.)

And it refuses outright, before publishing anything, if the robot's live
fleet session is currently **connected** (not merely paused) -- pausing or
stopping the service first is required here, not just recommended, since
this run's fixture schema would otherwise reach that connected live
subscriber as a schema update, remapping live channel batches to the
fixture's `selftest.*` channels until the live service's next reconnect.
That check isn't just a start-of-run gate: it keeps watching for the rest
of the run, and aborts (same typed error, connection torn down silently,
no close beat) the instant it sees any live activity on the robot's
session topics -- not the heartbeat alone. A service resuming partway
through a run can have its heartbeat thread stuck behind the selftest's
own spool lock while its router still reconnects and republishes the
schema regardless, so both topics are watched, and a signal on either one
is enough on its own to abort.

## Dev rigs

`FLEET_DEV_INSECURE=1` permits a plaintext `mqtt://` broker **only** when its
host resolves to loopback or a private (RFC1918) address -- a production
robot can never silently fall back to an unencrypted public broker. An
unenrolled dev robot streams as the placeholder identity `dev/robot`.
Production is always `mqtts://` with an enrolled identity.
