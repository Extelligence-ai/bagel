#!/usr/bin/env bash
# One dependency policy for all service images; runtime must not resync it.
set -euo pipefail
dev_mode="$1"
shift
args=(--locked)
if [[ "$dev_mode" != true ]]; then
    args+=(--no-dev)
fi
# Format-, export- and upload-level features every image serves: Rerun export,
# MDF4/CAN sources, GCS/Azure upload tasks, Cloudini pointcloud tasks.
set -- "$@" viz automotive upload cloudini
# JEV_MODE (a Docker build arg) opts the image into the on-robot decision model.
if [[ "${JEV_MODE:-false}" == true ]]; then
    set -- "$@" jev
fi
for group in "$@"; do
    args+=(--group "$group")
done
uv sync "${args[@]}"
# Fail the build, not the user's tool call, if a shared group did not install.
uv run --no-sync python -c "import rerun, asammdf, cantools, can, wasmtime, azure.storage.blob, google.cloud.storage"
