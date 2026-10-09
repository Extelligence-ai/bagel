#!/usr/bin/env bash
# One dependency policy for all service images; runtime must not resync it.
set -euo pipefail
dev_mode="$1"
shift
# The package itself (bagel_mcp/) is copied in after this layer, so only the
# locked dependencies are installed here; `uv run` resolves the package from
# the working directory like any other script.
args=(--locked --no-install-project)
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
# Feature sets are pip extras (`[project.optional-dependencies]`, shared with pip
# users); ros1/ros2 are dependency groups because their native half is apt-only.
for feature in "$@"; do
    case "$feature" in
        ros1 | ros2) args+=(--group "$feature") ;;
        *) args+=(--extra "$feature") ;;
    esac
done
uv sync "${args[@]}"
# Fail the build, not the user's tool call, if a shared group did not install.
uv run --no-sync python -c "import rerun, asammdf, cantools, can, wasmtime, azure.storage.blob, google.cloud.storage"
