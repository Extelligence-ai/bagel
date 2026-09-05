#!/usr/bin/env bash
# One dependency policy for all service images; runtime must not resync it.
set -euo pipefail
dev_mode="$1"
shift
args=(--locked)
if [[ "$dev_mode" != true ]]; then
    args+=(--no-dev)
fi
for group in "$@"; do
    args+=(--group "$group")
done
uv sync "${args[@]}"
