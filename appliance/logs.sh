#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib.sh"
SERVICE="${1:-aiops-api}"

cd "${COMPOSE_DIR}"
docker compose --env-file "${ENV_FILE}" logs -f --tail=120 "${SERVICE}"
