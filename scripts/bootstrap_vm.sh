#!/usr/bin/env bash
# One-time setup for a fresh Ubuntu VM (Oracle Cloud Always Free Ampere A1 recommended).
# Usage: bash bootstrap_vm.sh https://github.com/YOUR_USER/lakewire.git
set -euo pipefail
REPO_URL="${1:?usage: bootstrap_vm.sh <git-repo-url>}"

if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
fi
sudo systemctl enable --now docker

sudo mkdir -p /opt/lakewire && sudo chown "$USER":"$USER" /opt/lakewire
[ -d /opt/lakewire/.git ] || git clone "$REPO_URL" /opt/lakewire
cd /opt/lakewire
[ -f .env ] || { cp .env.example .env; echo ">> Edit /opt/lakewire/.env (passwords, USER_AGENT) before first start."; }

# Docker restarts containers on reboot via restart: unless-stopped. A cron safety net checks health hourly.
( crontab -l 2>/dev/null | grep -v lakewire-health ; echo "7 * * * * cd /opt/lakewire && python3 scripts/healthcheck.py >> /var/log/lakewire-health.log 2>&1 # lakewire-health" ) | crontab -
echo "Log out and back in once so the docker group applies, then: cd /opt/lakewire && docker compose up -d --build"
