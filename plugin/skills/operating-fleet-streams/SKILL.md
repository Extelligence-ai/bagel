---
name: operating-fleet-streams
description: Use when enrolling a robot with a fleet service, streaming live channels/events/heartbeats to a fleet broker, pausing or stopping fleet streaming, or diagnosing spool/queue/certificate questions.
---

# Operating fleet streams

A robot enrolls once with a fleet service (mTLS identity on disk), then
streams selected channels, events and heartbeats to a fleet broker with
store-and-forward spooling -- an unreliable link delays data, never loses it.

- Start every diagnosis with `describe_stream_status`: it is the local source
  of truth and answers sanely in every state (not installed, disabled,
  unenrolled, stopped, paused, running) without needing the broker reachable.
- Workflow: `describe_stream_status` -> `enroll_fleet_identity` (one-time
  token + enroll URL) -> `stream_live_topics` to add/replace channel and
  event rules -> `pause_fleet_streaming`/`resume_fleet_streaming` for
  temporary offline -> `stop_live_streams` to remove rules ->
  `unenroll_fleet_identity` to leave the fleet entirely (deletes the identity
  file set and the manifest's `streams:` section; re-enrolling needs a fresh
  token).
- Rules applied via `stream_live_topics` persist to the startup manifest's
  `streams:` section and survive restarts:

  ```yaml
  streams:
    broker: mqtts://fleet.example.com:8883   # optional; defaults to the
                                             # enrolled identity's broker_url
    flush_interval_s: 1.0                    # optional; default 1.0
    channels:
      - topic: "robot/telemetry"
        fields: ["speed", "battery.percent"]
        rate_hz: 5
    events:
      - name: low_battery
        topic: "robot/telemetry"
        predicate: "\"robot/telemetry\"['battery']['percent'] < 10"
  ```

  A rule change restarts the streaming service (brief reconnect; the spool
  preserves data across it).
- Kill switch: `FLEET_ENABLED=0` makes the subsystem fully inert -- no
  connections, no first-boot enrollment, tools refuse. Only
  `describe_stream_status` and `unenroll_fleet_identity` still work.
- Spool/queue questions: the channels lane is capped by
  `FLEET_SPOOL_MAX_BYTES` (evict-oldest; the heartbeat's `spool.evicted`
  counts losses); events and heartbeats are never dropped. Certificate
  renewal is automatic from 30 days before expiry (`cert_expires_at` in
  status). See references/settings-knobs.md for every `FLEET_*` knob.
- Deeper dives: `doc/runbooks/fleet_streaming.md` (operator runbook) and
  `doc/fleet_protocol_v1.md` (the v1 wire contract a fleet service consumes).
