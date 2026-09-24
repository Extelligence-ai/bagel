#!/usr/bin/env bash
# One dependency policy for all service images; runtime must not resync it.
set -euo pipefail
dev_mode="$1"
shift
args=(--locked)
if [[ "$dev_mode" != true ]]; then
    args+=(--no-dev)
fi
# JEV_MODE (a Docker build arg) opts the image into the on-robot decision model.
if [[ "${JEV_MODE:-false}" == true ]]; then
    set -- "$@" jev
fi
for group in "$@"; do
    args+=(--group "$group")
done
uv sync "${args[@]}"
