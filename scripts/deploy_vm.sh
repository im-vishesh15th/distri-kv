#!/usr/bin/env bash
# Build (on this VM) and start the production stack. Run from anywhere; it cds to the
# repo root. Re-running is the normal way to update after `git pull`.
#
#   ./scripts/deploy_vm.sh              # build image, start/refresh the stack, wait for health
#   ./scripts/deploy_vm.sh --no-build   # reuse the image already loaded on this VM
#
# NEVER deletes data: it uses `up -d` only (no `down`, no `-v`, no volume prune).
set -euo pipefail
. "$(dirname "$0")/prod_common.sh"
cd "$PROD_ROOT"

BUILD=1
for a in "$@"; do case "$a" in --no-build) BUILD=0;; -h|--help) sed -n 2,9p "$0"; exit 0;; *) echo "unknown option $a" >&2; exit 2;; esac; done

echo "== prerequisites"
command -v docker >/dev/null || { echo "docker not found: run scripts/setup_azure_vm.sh first" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "cannot talk to docker: are you in the docker group (log out/in) ?" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "docker compose plugin missing: run scripts/setup_azure_vm.sh" >&2; exit 1; }
[ -f deployments/shard.json ] || { echo "deployments/shard.json missing" >&2; exit 1; }

if [ ! -f "$PROD_ENV_FILE" ]; then
  cp deployments/.env.example "$PROD_ENV_FILE"; chmod 600 "$PROD_ENV_FILE"
  echo "Created $PROD_ENV_FILE from the template. Edit DOMAIN and CONTROL_ORIGINS, then re-run." >&2
  exit 1
fi
DOMAIN=$(env_value DOMAIN); ORIGINS=$(env_value CONTROL_ORIGINS)
[ -n "$DOMAIN" ] && [ -n "$ORIGINS" ] || { echo "DOMAIN and CONTROL_ORIGINS must be set in $PROD_ENV_FILE" >&2; exit 1; }
case "$ORIGINS" in *your-console.vercel.app*) echo "note: CONTROL_ORIGINS is still the placeholder; browsers cannot use the control API until you set your real console URL." ;; esac
[ "$(grep -c 'groups' deployments/shard.json)" -ge 1 ] && echo "shard config: $(tr -d '\n ' < deployments/shard.json)"

echo "== validating compose file"
compose config -q

if [ "$BUILD" -eq 1 ]; then
  mem_mb=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
  swap_mb=$(awk '/SwapTotal/ {print int($2/1024)}' /proc/meminfo)
  if [ $((mem_mb + swap_mb)) -lt 2500 ]; then
    echo "WARNING: only ${mem_mb} MiB RAM + ${swap_mb} MiB swap. The Go build can be OOM-killed." >&2
    echo "         Run scripts/setup_azure_vm.sh (adds swap) or build elsewhere and use --no-build (docs/deploy-azure.md)." >&2
  fi
  echo "== building image distrikv:latest (first build is slow on 2 vCPU: expect 10-25 minutes)"
  compose build
else
  docker image inspect distrikv:latest >/dev/null 2>&1 || { echo "--no-build given but image distrikv:latest is not on this VM (see docs/deploy-azure.md, option B)" >&2; exit 1; }
  echo "== using existing image distrikv:latest"
fi

echo "== starting stack (up -d; existing volumes are reused)"
compose up -d

echo "== waiting for health (max 5 min)"
deadline=$(( $(date +%s) + 300 ))
for c in distrikv-1 distrikv-2 distrikv-3 distrikv-gateway distrikv-proxy; do
  while [ "$(container_health "$c")" != healthy ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo "TIMEOUT: $c is '$(container_health "$c")'. See: docker logs $c" >&2; compose ps; exit 1; }
    sleep 3
  done
  echo "healthy: $c"
done

echo
compose ps
echo
echo "Stack is up. Next:"
echo "  ./scripts/healthcheck_prod.sh      # full verification"
echo "  ./scripts/raft_status_prod.sh      # who leads which Raft group"
