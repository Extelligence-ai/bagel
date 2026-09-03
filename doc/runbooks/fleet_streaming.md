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
channel rules are, and `stream_live_topics`/`stop_live_streams` report them
back as `events_configured` -- but on-robot evaluation ships in a later
release; configuration is forward-compatible, not yet active
(`events_active` is always `False`).

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
`pause_fleet_streaming(discard=True)`.

## Dev rigs

`FLEET_DEV_INSECURE=1` permits a plaintext `mqtt://` broker **only** when its
host resolves to loopback or a private (RFC1918) address -- a production
robot can never silently fall back to an unencrypted public broker. An
unenrolled dev robot streams as the placeholder identity `dev/robot`.
Production is always `mqtts://` with an enrolled identity.
