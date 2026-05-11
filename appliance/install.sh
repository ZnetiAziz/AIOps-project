#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
SERVICE_FILE="${SCRIPT_DIR}/aiops-appliance.service"
SYSTEMD_TARGET="/etc/systemd/system/aiops-appliance.service"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required. Install Docker Engine and the Docker Compose plugin first."
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose plugin is required. Install docker-compose-plugin first."
  exit 1
fi

if [ ! -f "${PROJECT_DIR}/.env" ]; then
  cp "${PROJECT_DIR}/.env.example" "${PROJECT_DIR}/.env"
  echo "Created ${PROJECT_DIR}/.env from .env.example"
fi

sed "s#__PROJECT_DIR__#${PROJECT_DIR}#g" "${SERVICE_FILE}" | sudo tee "${SYSTEMD_TARGET}" >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable aiops-appliance.service

"${SCRIPT_DIR}/start.sh"

echo
echo "Installed aiops-appliance.service."
echo "Use: sudo systemctl status aiops-appliance"
