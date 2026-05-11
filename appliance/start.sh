#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib.sh"

ensure_env_file
check_docker_ready

cd "${COMPOSE_DIR}"
docker compose --env-file "${ENV_FILE}" up -d

echo
echo "AIOps appliance started."
echo "Dashboard:    http://$(get_primary_ip):$(grep -E '^DASHBOARD_PORT=' "${ENV_FILE}" | cut -d= -f2 || echo 8088)"
echo "API docs:     http://$(get_primary_ip):8000/docs"
echo "Grafana:      http://$(get_primary_ip):3001"
echo "Prometheus:   http://$(get_primary_ip):9090"
