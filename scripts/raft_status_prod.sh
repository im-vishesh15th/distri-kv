#!/usr/bin/env bash
# Show which node leads each Raft group (0, 1, 2, metadata) and each node's commit
# index, read from the nodes' private /metrics endpoint. Read-only.
#
#   ./scripts/raft_status_prod.sh           # once
#   ./scripts/raft_status_prod.sh --watch   # refresh every 2 s (Ctrl+C to stop)
set -uo pipefail
. "$(dirname "$0")/prod_common.sh"

if [ "${1:-}" = "--watch" ]; then
  while :; do clear; date; echo; raft_table; sleep 2; done
fi
raft_table
