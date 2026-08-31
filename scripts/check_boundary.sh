#!/usr/bin/env bash
# Boundary rule: Bagel speaks to a generic "fleet broker" / "fleet service" and
# never names a specific fleet product in source, docs, settings or tool text.
# Product-specific integration tests live in the product's own repo.
set -euo pipefail
cd "$(dirname "$0")/.."
# Case-insensitive; extend the alternation if a new vendor name must stay out.
# The term list is octal-encoded so the guarded names never appear in this
# public tree (or its grep results) in plaintext.
PATTERN="$(printf '\155\141\164\143\150\141')"
if hits=$(git grep -n -i -E "$PATTERN" -- ':!docs/**' ':!scripts/check_boundary.sh'); then
  echo "Boundary rule violated (see AGENTS.md): vendor names are not allowed in Bagel." >&2
  echo "$hits" >&2
  exit 1
fi
echo "boundary check passed"
