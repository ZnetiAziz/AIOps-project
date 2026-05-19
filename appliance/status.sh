#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib.sh"

cd "${COMPOSE_DIR}"
docker compose --env-file "${ENV_FILE}" ps

echo
echo "Firewall:"
if command -v nft >/dev/null 2>&1; then
  "${SCRIPT_DIR}/firewall.sh" status || echo "Firewall status unavailable."
else
  echo "nft command not found. Firewall appliance mode is not installed on this host."
fi
