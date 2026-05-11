#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
BACKUP_DIR="${PROJECT_DIR}/backups"
STAMP="$(date +%Y%m%d-%H%M%S)"
ARCHIVE="${BACKUP_DIR}/aiops-appliance-${STAMP}.tar.gz"

mkdir -p "${BACKUP_DIR}"

cd "${PROJECT_DIR}"
tar \
  --exclude='./.git' \
  --exclude='./backups' \
  --exclude='./aiops-api/__pycache__' \
  -czf "${ARCHIVE}" \
  .

echo "Backup created: ${ARCHIVE}"
