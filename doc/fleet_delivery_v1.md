# Approved capture delivery (v1)

The fleet branch includes an opt-in delivery supervisor in
`src.sink.publish.delivery`. It runs alongside the existing MQTT fleet publisher
and MCP server; enrollment and renewal still belong to the existing identity
implementation. Each controller has one managed capture slot. Existing local
startup pipelines and streaming rules are independent.

Enable it with `FLEET_DELIVERY_SOURCES` pointing to a local JSON file. Persistent
state defaults to `~/.bagel/delivery` (`FLEET_DELIVERY_DIRECTORY`). The identity
comes from `FLEET_IDENTITY_DIRECTORY`; the HTTPS mTLS control base defaults to the
identity's `renew_url`, or an explicit `FLEET_CONTROL_URL`. The supervisor reloads
certificate pointers per request. Do not run a second supervisor for the same
installation; an exclusive state-directory lock prevents duplicate local use.

`FLEET_ENABLED=0` disables delivery as well as publishing. Unenrollment stops the
supervisor and workers before removing identity files. Server exit also stops
the process group. A credential denial is journaled and prevents offline restore
until the gateway authorizes the identity again. An already-active capture can
continue during a network outage; a new deployment requires an online check.

Example operator-owned source config (credentials stay on the robot):

```json
{"sources":{"sensors":{"sink":"mqtt","host":"sensor-broker","port":1883,
"topics":["sensors/temperature"],"args":{"discovery_seconds":2}}},
"buffer_bytes_per_topic":16777216,"artifact_quota_bytes":536870912}
```

Mount source configuration read-only and identity/state on persistent volumes.
The optional `compose.fleet-delivery.yaml` override provides these mounts for
`ros2-kilted` and `iot`. The container user must be able to write state and read
identity; use a restart policy and allow at least 25 seconds for shutdown.

## Protocol

- `GET /v1/control/next` returns the authenticated identity, server time and latest
  desired job (or null). A job includes tenant, robot, asset, installation,
  credential generation, target ID, revision, last report sequence, expiry,
  release digest, runtime identifier and inline capture spec.
- `POST /v1/control/report` submits target/revision, persistent sequence, phase,
  actual observed digest, detail and latest execution evidence. HTTP 409 forces
  reconciliation before activation; HTTP 403 stops execution durably.
- The spec schema is `bagel.capture.v1`, runtime `bagel-standing-v1`. Canonical
  JSON uses sorted keys, compact separators, ASCII escaping and finite numbers;
  the digest is SHA-256 over its UTF-8 bytes. `contract.py` defines validation.
  No product-specific server code or database driver is bundled here.

Capture specs accept an explicit local source alias, 1–8 allowlisted topics,
periodic cadence or a numeric rising-edge field comparison, a bounded lookback,
and CSV/Parquet output. Arbitrary YAML, module paths, SQL, credentials, upload
endpoints and software installers are refused. Only the known native capture
task is compiled. The fixture in `test/sink/publish/delivery/contract-v1.json`
lets a fleet service test protocol compatibility without importing this runtime.

Preparation discovers topics and builds a worker before stopping the old worker.
Activation registers subscriptions and then reports `active`; it does not claim
healthy robot operation. Execution evidence separately contains local paths,
file sizes and content hashes. Captures stay local and are never auto-deleted.
The storage limit is an admission watermark and may be exceeded by one capture;
use filesystem quotas for a hard limit. Handoffs can have a collection gap.

Restart restores only the durable active release, never a pending one. Rollback
is a new approved server revision targeting an earlier release. This is capture
delivery, not general software/configuration installation or an OTA service.

## Build and verification

The normal Dockerfiles copy the delivery package with the rest of `src` and start
it through `server.py` when configured. For development with a cached fleet
platform image, `docker/Dockerfile.fleet-review` copies this branch's source and
refuses to build unless both `pyproject.toml` and `uv.lock` exactly match the
platform image. It never combines files from a different source branch. Only pass a commit
reference for a clean checkout; uncommitted review builds omit that reference.
The review image clears inherited platform-image provenance.

```sh
docker build --platform linux/amd64 -f docker/Dockerfile.fleet-review \
  --build-arg BAGEL_VCS_REF="$(git rev-parse HEAD)" \
  -t bagel-fleet-delivery:review .
```

The host suite covers contract vectors, expired/foreign/superseded requests,
lost acknowledgments, durable recovery, activation failure cleanup, the fleet
kill switch, rotated certificate pointers and stop-before-unenroll ordering.
Tests live under `test/sink/publish/delivery`, already included by the host CI
job's `test/sink` path. Product integration tests belong in the fleet service's
repository and should exercise mTLS against this branch image.

This is local development work, not a published version or production rollout.
Capture contents still remain local; ROS2 hardware acceptance and a supported
production install/recovery path remain open.
