#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
SERVICE="${1:-aiops-api}"

cd "${PROJECT_DIR}/docker"
docker compose --env-file "${PROJECT_DIR}/.env" logs -f --tail=120 "${SERVICE}"
