# Bagel fleet wire protocol v1

This document is the binding v1 contract between a Bagel robot and a fleet service.
Everything below is stated as built: a fleet service can pin automated conformance
probes to this page. Additive change (a new OPTIONAL field, a new topic kind) does not
bump the version; any breaking change ships as `bagel/v2/...` alongside v1.

Consumers MUST ignore unknown fields in every payload.

## 1. Versioning and namespace

All traffic for one robot lives under one topic subtree:

```
bagel/v1/{tenant}/{robot}/{kind}
```

where `{kind}` is one of `schema`, `channels`, `events`, `heartbeat`, `cmd`.
`cmd` is reserved for a future release and is not published or subscribed in v1.

- Transport is MQTT v5. Payloads are JSON, UTF-8. The robot emits compact JSON
  (no insignificant whitespace), but that is an emission detail, not a parsing
  contract.
- Every publish is QoS 1 (see §3). The robot never publishes at QoS 0 or 2.

## 2. Identity and authorization

- The robot authenticates with mTLS. Its client certificate's CN is
  `{tenant}/{robot}`, assigned and signed by the fleet service at enrollment
  (the CSR's own CN is advisory only and is ignored).
- Broker ACLs restrict each certificate to its own
  `bagel/v1/{tenant}/{robot}/#` subtree. Because identity is carried entirely
  by the certificate and the topic, **payloads contain no tenant or robot
  fields**.
- The MQTT client id is `bagel/{tenant}/{robot}`. Neither id ever contains `/`
  (robot ids match `^[a-z0-9][a-z0-9_-]{0,62}$`), so the delimiter is
  unambiguous. The client id is deterministic per robot: a reconnecting robot
  displaces its own stale session rather than leaking a second one.
- MQTT keepalive is 30 s.

## 3. QoS and retain

| kind        | QoS | retained |
|-------------|-----|----------|
| `schema`    | 1   | yes      |
| `channels`  | 1   | no       |
| `events`    | 1   | no       |
| `heartbeat` | 1   | yes      |
| LWT (on `heartbeat`) | 1 | yes |

A late subscriber therefore always sees the current schema and the robot's
last-known liveness state immediately; channel batches and events are only
seen live (plus at-least-once replay, §6).

## 4. Last will and offline reasons

At connect time the robot arms a retained last-will on its `heartbeat` topic:

```json
{"v": 1, "online": false, "reason": "lwt"}
```

An ungraceful drop (crash, power loss, network partition) makes the broker
publish it after the keepalive lapses. The LWT payload carries no `t` field.

A graceful disconnect publishes a retained clean-stop heartbeat before closing:

```json
{"v": 1, "t": <epoch seconds>, "online": false, "reason": "stopped"}
```

`reason` is `"stopped"` for a stop or shutdown and `"paused"` for a deliberate
pause -- a paused robot is distinguishable from a stopped one by the retained
payload alone. Consumers MUST treat any `online: false` heartbeat as
offline regardless of `reason`, and MUST tolerate new `reason` values.

## 5. `schema` payload

```json
{
  "v": 1,
  "channels": [
    {
      "c": "telemetry.battery.percent",
      "type": "number",
      "unit": "%",
      "source_topic": "robot/telemetry",
      "source_field": "battery.percent"
    }
  ]
}
```

- `c`: the channel name every `channels` sample refers to (unique per robot).
- `type`: one of `number`, `string`, `bool`, `geo`.
- `unit`: a unit string or `null` (source metadata is often absent).
- `source_topic` / `source_field`: provenance on the robot. For a `geo`
  channel, `source_field` is the comma-joined component map (e.g.
  `"lat=lat,lon=lon"`).

The schema is republished (retained) on **every** (re)connect, and a rule
change on the robot restarts its streaming session -- reconnect, then a fresh
schema -- so the retained schema always describes the samples that follow it.
Consumers must tolerate re-receiving an identical schema.

## 6. `channels` payload and seq semantics

```json
{
  "v": 1,
  "seq": 42,
  "t_batch": 1767312000.5,
  "samples": [
    {"c": "telemetry.battery.percent", "t": 1767312000.1, "v": 87.5},
    {"c": "telemetry.gps.geo", "t": 1767312000.2,
     "v": {"lat": 52.5, "lon": 13.4, "alt": 34.2}}
  ]
}
```

- `t_batch`: epoch seconds when the batch was flushed. `t`: the sample's
  source timestamp, epoch seconds. Both are floats.
- `v` is typed per the schema: JSON number for `number`, string for `string`,
  boolean for `bool`, and for `geo` an object `{"lat": <num>, "lon": <num>}`
  with an OPTIONAL `"alt"` key (present only when the robot's rule mapped an
  altitude field).
- Sample density: each channel is rate-capped on the robot at its configured
  `rate_hz` (at most one sample per `1/rate_hz` interval, capped at 50 Hz).

**Batching.** The robot flushes a batch every `flush_interval_s` (default
1 s) or as soon as 500 samples are pending, whichever comes first. A batch
carries at most 500 samples; a wider backlog is split into consecutive
batches in the same flush pass, each with its own `seq`.

**seq.** Monotonic per lane (`channels` and `events` are separate seq
spaces), per robot, starting at 1, persisted on the robot's disk spool across
restarts -- a rebooted robot continues its seq, it never restarts from 1.

**Delivery.** At-least-once: QoS 1 plus spool replay after reconnect means a
consumer can see a batch twice. The dedupe key is
`(tenant, robot, kind, seq)` -- tenant and robot from the topic, `kind` the
topic's last segment. Replay is strictly seq-ordered: batches never arrive
out of order within a lane. A **gap** in the `channels` seq therefore means
exactly one thing: the robot's capped spool evicted oldest batches during a
sustained disconnect (reported in the heartbeat's `spool.evicted`, §8) --
never reordering.

## 7. `events` payload

```json
{
  "v": 1,
  "seq": 7,
  "event_id": "e0a1...",
  "name": "low_battery",
  "t_start": 1767312000.0,
  "t_end": 1767312030.0,
  "source_topic": "robot/telemetry",
  "summary": {},
  "artifact": {"kind": "mcap", "uri": "..."}
}
```

- Events have their **own lane and seq space**, independent of `channels`,
  with the same at-least-once + seq-ordered semantics and the same dedupe key.
  The events lane is never evicted: an event, once recorded, is delivered.
- `event_id` is a globally unique id; `name` is the robot-side rule name;
  `summary` is a free-form JSON object.
- `artifact` is OPTIONAL. When present, `kind` is `"mcap"` (the only v1 kind)
  and `uri` points at the captured artifact.

The on-robot event *emitter* ships in a future release; this shape is
contractual now, and the conformance selftest (§10) already publishes one
event (without `artifact`).

## 8. `heartbeat` payload

Published retained every **30 s** (first beat immediately at session start):

```json
{
  "v": 1,
  "t": 1767312000.0,
  "online": true,
  "bagel_version": "2.2.3",
  "uptime_s": 3600.5,
  "subscriptions": ["robot/telemetry"],
  "channels_active": 3,
  "queue": {"depth": 0, "dropped": 0},
  "spool": {"bytes": 12288, "pending": 0, "evicted": 0},
  "disk_free_bytes": 52000000000,
  "cert_expires_at": "2026-12-01T00:00:00Z",
  "reconnects": 2
}
```

- `bagel_version`: version string, or `"unknown"` when the robot cannot
  determine its own version.
- `uptime_s`: seconds since the streaming session started.
- `subscriptions`: the source topics currently tapped; `channels_active`: the
  number of resolved channels in the current schema.
- `queue`: the robot's in-memory sample queue -- `depth` now, `dropped`
  cumulative (oldest-dropped on overflow).
- `spool`: on-disk store-and-forward totals summed across lanes -- `bytes` on
  disk, `pending` records not yet acknowledged by the broker, and
  **`evicted`: the eviction indicator.** A nonzero (or growing) `evicted`
  count is how data loss is reported: it counts batches discarded from the
  capped `channels` lane under sustained disconnect, and it is exactly what a
  consumer's observed gaps in the `channels` seq (§6) correspond to.
- `disk_free_bytes`: free space on the robot's cache filesystem.
- `cert_expires_at`: ISO-8601 expiry of the robot's client certificate, or
  `null` until the robot is enrolled. Consumers MUST tolerate `null`.
- `reconnects`: cumulative unexpected disconnects this session.

**OPTIONAL, reserved:** a `build` block --
`"build": {"build_id": "...", "vcs_ref": "..."}` -- is reserved for a future
release. Consumers MUST tolerate both its absence and its arrival.

Heartbeats are point-in-time liveness: a beat that cannot be published while
offline is recorded to the robot's spool for diagnostics but is not replayed
to the broker -- the retained heartbeat/LWT is the liveness source of truth.

## 9. Enrollment and renewal protocol

Both endpoints take JSON bodies and return JSON. `enroll_url` and `renew_url`
are **BASE** URLs: the client appends the `/v1/...` path itself (a trailing
slash on the base is tolerated). They may point at entirely different hosts.

### Enroll

```
POST {enroll_url}/v1/enroll
{"token": "<one-time token>", "csr_pem": "<PEM CSR>"}
```

- The private key is generated on the robot and never leaves it; only the CSR
  crosses the network. The token appears in the request body only -- the
  client never logs or stores it.
- `200` response:

```json
{
  "cert_pem": "...", "ca_pem": "...", "broker_url": "mqtts://...",
  "tenant": "...", "robot_id": "...", "expires_at": "2026-12-01T00:00:00Z",
  "renew_url": "https://..."
}
```

  The first six fields are required. `renew_url` is **OPTIONAL**: when
  present it is the BASE the client's future renewals target; when absent the
  client renews against `enroll_url`.
- Error statuses: `401` = unknown token; `410` = token already used or
  expired. Tokens are single-use.
- `expires_at` (here and in renew) is ISO-8601 with an explicit timezone
  (`Z` accepted).

### Renew

```
POST {renew_url or enroll_url}/v1/renew        (over mTLS)
{"csr_pem": "<PEM CSR>"}
```

- Authenticated by the robot's CURRENT client certificate over mTLS; no token.
  The CSR is backed by a brand-new private key every time.
- `200` response:

```json
{"cert_pem": "...", "expires_at": "...", "ca_pem": "...", "renew_url": "..."}
```

  `cert_pem` and `expires_at` are required. `ca_pem` is OPTIONAL (present
  only when the CA is being rotated). `renew_url` is **OPTIONAL here too**,
  with the same BASE semantics: present means the fleet service is rotating
  the base future renewals target (without re-enrollment); **absent means no
  change** to whatever base the client already uses.
- `404` or `501` = renewal not offered on this deployment; the client treats
  this as expected (quiet), keeps its current certificate, and retries later.

**Client timing** (what a fleet service can rely on): renewal attempts begin
30 days before `expires_at`, are rate-limited to at most one attempt per day
(persisted across robot restarts), and use a 30 s HTTP timeout. A failed
renewal never damages the stored identity. The live MQTT session keeps its
old certificate until its next reconnect.

## 10. Conformance

What a fleet service MUST accept: everything in §§1-8, including duplicate
batches (dedupe on seq), seq gaps on `channels` (evictions), re-published
identical schemas, `null` units and `null` `cert_expires_at`, unknown fields,
and all three offline `reason` values.

To validate an ingestion pipeline without a robot, run the selftest against
the target broker from any Bagel checkout or image with the fleet group
installed:

```bash
uv run python -m src.sink.publish.selftest [--broker mqtt(s)://...] \
    [--batches N] [--interval-s S]
```

It publishes, in order, using the enrolled identity (or `--broker` on a dev
rig): one retained schema, N channel batches (default 10, default 0.5 s
apart), one heartbeat, one event -- then prints a one-line JSON summary and
exits 0 (any expected failure prints to stderr and exits 1). It requires both
spool lanes (`channels`, `events`) to be fully drained (no pending unacked
entries) before it will start -- it allocates and acks real seqs on those
lanes, so pending backlog would otherwise be silently advanced past; run it
with fleet streaming paused or stopped.

The fixture is fixed and deterministic. The schema is exactly these four
channels:

| c | type | unit | source_topic | source_field |
|---|------|------|--------------|--------------|
| `selftest.number` | `number` | `"1"` | `selftest` | `number` |
| `selftest.bool`   | `bool`   | `null` | `selftest` | `bool` |
| `selftest.string` | `string` | `null` | `selftest` | `string` |
| `selftest.geo`    | `geo`    | `null` | `selftest` | `lat=lat,lon=lon` |

Every batch carries exactly 4 samples (one per channel; the batch size is
fixed). For batch index `i` (0-based) at publish time `t`:

- `selftest.number`: `v = float(i)`
- `selftest.bool`: `v = (i % 2 == 0)`
- `selftest.string`: `v = "selftest-{i}"`
- `selftest.geo`: `v = {"lat": 52.0 + i*0.001, "lon": 13.0 + i*0.001}`

The heartbeat reports `subscriptions: ["selftest"]` and `channels_active: 4`.
The event has `name: "selftest"`, an `event_id` of the form
`selftest-<uuid4>`, a `summary` that contains at least
`{"kind": "selftest", "batches": N}` (future fields may be added; existing
readers must not require an exact match), and no `artifact`. Seqs come from
the robot's real lanes, so they continue from whatever the robot last used
rather than starting at 1.
