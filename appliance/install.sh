#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib.sh"
SERVICE_FILE="${SCRIPT_DIR}/aiops-appliance.service"
SYSTEMD_TARGET="/etc/systemd/system/aiops-appliance.service"

check_docker_ready
check_host_capacity 8192 15
ensure_env_file

sed "s#__PROJECT_DIR__#${PROJECT_DIR}#g" "${SERVICE_FILE}" | sudo tee "${SYSTEMD_TARGET}" >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable aiops-appliance.service

"${SCRIPT_DIR}/start.sh"

echo
echo "Installed aiops-appliance.service."
echo "Use: sudo systemctl status aiops-appliance"
