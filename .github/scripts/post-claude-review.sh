#!/usr/bin/env bash
# Narrow wrapper around `gh pr review`, invoked by Claude's Bash tool in
# claude-review.yaml. Deliberately accepts only two positional arguments --
# a PR number and an inline body string -- with no equivalent of
# `gh pr review --body-file`/`-F`. That flag lets `gh` itself read an
# arbitrary local file and publish its contents as the review body,
# bypassing every path-scoped Read/Grep/Glob rule and --disallowedTools
# entry (`gh`'s own file I/O isn't mediated by Claude Code's tool
# permissions at all). Routing review submission through this script
# instead of a raw `gh pr review` Bash permission removes that vector
# without removing Claude's ability to post the review itself.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <pr-number> <body-text>" >&2
  exit 1
fi

pr="$1"
body="$2"

exec gh pr review "$pr" --comment --body "$body"
