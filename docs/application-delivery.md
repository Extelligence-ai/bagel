# Optional application worker

Application delivery ships with Bagel as a separately supervised worker. Enable
it when provisioning a robot for application updates; ordinary telemetry and
capture do not require Docker-engine access. The worker shares the existing
fleet identity and reloads renewed certificates on each request. Remote users
select enrolled robots through their fleet service, without entering robot IPs
or running terminal commands for each release.

## Installation profile

`compose.applications.yaml` adds the worker to an installation. Set:

- `BAGEL_APPLICATION_BASE`: the matching fleet-enabled Bagel image for this
  source revision (the build checks that dependency manifests match).
- `BAGEL_IDENTITY_DIRECTORY`: existing enrolled identity directory, mounted
  read-only. Certificate paths in identity.yaml must resolve inside the worker.
- `BAGEL_APPLICATION_CONFIG`: operator-owned JSON file, mounted read-only.
- `BAGEL_APPLICATION_READY_DIRECTORY`: operator-owned readiness directory,
  mounted read-only at `/readiness`.

Example local profile:

```json
{
  "applications": {
    "inspection": {
      "registry_prefixes": ["registry.example.com/robotics/"],
      "ready_file": "/readiness/inspection",
      "health_timeout_seconds": 120,
      "memory_bytes": 1073741824,
      "model_max_bytes": 536870912,
      "model_read_timeout_seconds": 120
    }
  }
}
```

The readiness file must contain `ready` before activation or recovery. On
hardware, the integration owning maintenance mode must produce that signal.
The worker does not determine physical safety. An empty or missing signal blocks
container changes. Keep the persistent application-state volume across upgrades.
One worker exclusively locks its state directory. Changing enrollment identity
requires explicit local reconciliation of its existing applications.

The worker needs Docker-engine privileges; remote application containers do
not receive its socket or its identity. They use a read-only root filesystem,
no extra capabilities, no-new-privileges, bounded PIDs/memory and a small /tmp
volume. Release manifests cannot add commands, host mounts or devices. This
first profile does not expose GPUs, robot devices or application service ports.
It is suitable for background services with outbound connections; extend local
profiles deliberately for other application requirements.

Private registries require an operator-provisioned Docker credential config
(or credential helper and its credentials) in the worker. Do not include tokens
in remote artifact references. The example compose file has no credentials and
works with public registries; add a read-only credential mount during operator
provisioning where needed.

## Artifact contract

Runtime `docker-application-v1` uses the Docker daemon's Linux AMD64/ARM64
platform, including Docker Desktop's Linux VM. Software must be pinned by OCI
SHA-256 digest and include a HEALTHCHECK that verifies useful application
behavior. Models can be bundled in that image, in which case no separate model
release is needed.

An external model is an OCI image containing `/model/model.bin`, pinned by digest.
The worker creates, but never starts, a seed container. It checksums that file
without extracting arbitrary archive paths, then mounts the model volume
read-only at `/opt/fleet-model` and sets
`FLEET_MODEL_PATH=/opt/fleet-model/model.bin` for the application. Model-only
updates restart the application so it loads the selected model. Software and
model declare the same interface ID. Coordinated updates and rollback operate
on the complete resolved software/model pair.

The worker stages artifacts before stopping the old application. It rechecks
local inventory/readiness and server authorization after downloading. A durable
journal precedes stop/start effects. The candidate must pass HEALTHCHECK before
it is committed; otherwise the previous healthy container is restored.
Recovery on worker restart occurs before cloud contact and can finish offline.
An interrupted first install has no previous version and is reported as failed.
Failed/interrupted attempts require a new rollout to retry. Cancellation can
stop pending admission but cannot retract an activation already admitted.

Digests verify content integrity. Artifact signing, SBOM/vulnerability policy,
OS updates, and self-updating Bagel are not implemented here. Previous containers
and model volumes are retained for recovery; operators must plan disk capacity
and retention. Do not prune resources used by active or previous manifests.

## Verification

Host tests: `uv run pytest test/sink/publish/applications`.
The real-engine acceptance test additionally requires a disposable local OCI
registry and `BAGEL_TEST_REGISTRY=localhost:5017`. CI's `application-installer`
job supplies one and tests actual model loading, read-only mounts, health
failure rollback, checksum rejection and interrupted activation recovery.
