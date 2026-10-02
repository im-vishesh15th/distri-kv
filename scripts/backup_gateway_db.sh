#!/usr/bin/env bash
# Consistent backup of the gateway SQLite database (tenants, hashed API keys, console
# users and sessions) using `gatewayctl backup` (SQLite VACUUM INTO) inside the
# running gateway container. Safe while the gateway is serving traffic.
#
#   ./scripts/backup_gateway_db.sh                   # -> ~/distrikv-backups/
#   BACKUP_DIR=/mnt/backups KEEP=30 ./scripts/backup_gateway_db.sh
#
# This does NOT back up the Raft/KV data (the three node volumes). Those are
# replicated across the nodes, but all replicas live on this one VM; see
# docs/deploy-azure.md ("Backups") for disk snapshots.
set -euo pipefail
. "$(dirname "$0")/prod_common.sh"

BACKUP_DIR=${BACKUP_DIR:-$HOME/distrikv-backups}
KEEP=${KEEP:-14}
TS=$(date -u +%Y%m%dT%H%M%SZ)
NAME="gateway-$TS.db"

container_running distrikv-gateway || { echo "distrikv-gateway is not running" >&2; exit 1; }
umask 077
mkdir -p "$BACKUP_DIR"

docker exec distrikv-gateway gatewayctl -db "$GW_DB" backup "/var/lib/gateway/$NAME"
docker cp "distrikv-gateway:/var/lib/gateway/$NAME" "$BACKUP_DIR/$NAME"
docker exec distrikv-gateway rm -f "/var/lib/gateway/$NAME"

[ -s "$BACKUP_DIR/$NAME" ] || { echo "backup file is empty" >&2; exit 1; }
chmod 600 "$BACKUP_DIR/$NAME"
echo "backup written: $BACKUP_DIR/$NAME ($(wc -c < "$BACKUP_DIR/$NAME") bytes)"

# Keep the newest $KEEP backups.
ls -1t "$BACKUP_DIR"/gateway-*.db 2>/dev/null | tail -n +"$((KEEP+1))" | while read -r old; do rm -f -- "$old"; echo "removed old backup $old"; done
