#!/usr/bin/env bash
# One-time setup for a fresh Ubuntu VM (Oracle Cloud Always Free Ampere A1, or any Ubuntu host with 12 GB+ RAM).
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
if [ ! -f .env ]; then
  cp .env.example .env
  chmod 600 .env
  # Generate strong random secrets so no deployment ever runs on a placeholder password.
  sed -i "s|^S3_SECRET_KEY=.*|S3_SECRET_KEY=$(openssl rand -hex 24)|" .env
  sed -i "s|^GRAFANA_ADMIN_PASSWORD=.*|GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 24)|" .env
  echo ">> Generated passwords in /opt/lakewire/.env. Edit USER_AGENT (your contact email) before first start."
fi

# Docker restarts containers on reboot via restart: unless-stopped. A cron safety net checks health hourly.
( crontab -l 2>/dev/null | grep -v lakewire-health ; echo "7 * * * * cd /opt/lakewire && python3 scripts/healthcheck.py >> \$HOME/lakewire-health.log 2>&1 # lakewire-health" ) | crontab -
echo "Log out and back in once so the docker group applies, then: cd /opt/lakewire && docker compose up -d --build"
