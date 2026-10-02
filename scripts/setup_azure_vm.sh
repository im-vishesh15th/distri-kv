#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu 24.04 VM (tested target: Azure Standard_B2ats_v2,
# 2 vCPU / 1 GiB RAM). Installs ONLY: Docker Engine, the Compose plugin and Git,
# plus a swap file and Docker log rotation (both matter on a 1 GiB machine).
# Idempotent: running it twice is harmless. Does not touch existing Docker data.
#
#   ./scripts/setup_azure_vm.sh
#
# Afterwards log out and back in once so the `docker` group applies to your user.
set -euo pipefail

[ "$(id -u)" -ne 0 ] || { echo "Run as your normal user (azureuser); the script uses sudo where needed." >&2; exit 1; }
. /etc/os-release
[ "${ID:-}" = ubuntu ] || { echo "This script targets Ubuntu (found: ${ID:-unknown})." >&2; exit 1; }
SWAP_GB=${SWAP_GB:-2}

echo "== 1/5 base packages (ca-certificates, curl, git, gnupg)"
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl git gnupg

echo "== 2/5 Docker Engine + Compose plugin (official Docker apt repository)"
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  echo "docker and compose plugin already installed: $(docker --version)"
else
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME:-$VERSION_CODENAME} stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

echo "== 3/5 Docker log rotation (JSON Raft logs would otherwise fill the disk)"
if [ ! -f /etc/docker/daemon.json ]; then
  sudo mkdir -p /etc/docker
  printf '{\n  "log-driver": "json-file",\n  "log-opts": {"max-size": "10m", "max-file": "3"}\n}\n' | sudo tee /etc/docker/daemon.json >/dev/null
  sudo systemctl restart docker
else
  echo "/etc/docker/daemon.json already exists; left unchanged"
fi
sudo systemctl enable --now docker

echo "== 4/5 swap (${SWAP_GB} GiB): building the Go image needs more than 1 GiB"
if [ "$(swapon --show --noheadings | wc -l)" -gt 0 ]; then
  echo "swap already active:"; swapon --show
else
  sudo fallocate -l "${SWAP_GB}G" /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile >/dev/null
  sudo swapon /swapfile
  grep -q '^/swapfile ' /etc/fstab || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
  echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/99-distrikv-swap.conf >/dev/null
  sudo sysctl -q -p /etc/sysctl.d/99-distrikv-swap.conf
  echo "swap enabled"
fi

echo "== 5/5 docker group"
if id -nG "$USER" | grep -qw docker; then
  echo "$USER is already in the docker group"
else
  sudo usermod -aG docker "$USER"
  echo "added $USER to the docker group: LOG OUT AND BACK IN before running deploy_vm.sh"
fi

echo
echo "versions:"; sudo docker --version; sudo docker compose version; git --version
echo "memory:";   free -h | sed -n 1,3p
