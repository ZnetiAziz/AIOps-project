#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_DIR="${PROJECT_DIR}/docker"

if [ ! -f "${PROJECT_DIR}/.env" ]; then
  cp "${PROJECT_DIR}/.env.example" "${PROJECT_DIR}/.env"
  echo "Created ${PROJECT_DIR}/.env from .env.example"
fi

cd "${COMPOSE_DIR}"
docker compose --env-file "${PROJECT_DIR}/.env" up -d

echo
echo "AIOps appliance started."
echo "Dashboard:    http://$(hostname -I | awk '{print $1}'):$(grep -E '^DASHBOARD_PORT=' "${PROJECT_DIR}/.env" | cut -d= -f2 || echo 8088)"
echo "API docs:     http://$(hostname -I | awk '{print $1}'):8000/docs"
echo "Grafana:      http://$(hostname -I | awk '{print $1}'):3001"
echo "Prometheus:   http://$(hostname -I | awk '{print $1}'):9090"
