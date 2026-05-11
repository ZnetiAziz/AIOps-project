#!/usr/bin/env bash

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${PROJECT_DIR}/docker"
ENV_FILE="${PROJECT_DIR}/.env"
ENV_EXAMPLE="${PROJECT_DIR}/.env.example"

ensure_env_file() {
  if [ ! -f "${ENV_FILE}" ]; then
    cp "${ENV_EXAMPLE}" "${ENV_FILE}"
    echo "Created ${ENV_FILE} from .env.example"
  fi
}

require_command() {
  local cmd_name="$1"
  if ! command -v "${cmd_name}" >/dev/null 2>&1; then
    echo "Missing required command: ${cmd_name}"
    exit 1
  fi
}

check_docker_ready() {
  require_command docker

  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon is not reachable. Start Docker first."
    exit 1
  fi

  if ! docker compose version >/dev/null 2>&1; then
    echo "Docker Compose plugin is required."
    exit 1
  fi
}

check_host_capacity() {
  local min_ram_mb="${1:-8192}"
  local min_free_gb="${2:-15}"

  if command -v free >/dev/null 2>&1; then
    local total_ram_mb
    total_ram_mb="$(free -m | awk '/Mem:/ {print $2}')"
    if [ -n "${total_ram_mb}" ] && [ "${total_ram_mb}" -lt "${min_ram_mb}" ]; then
      echo "Warning: available RAM is ${total_ram_mb} MB. Recommended minimum is ${min_ram_mb} MB."
    fi
  fi

  if command -v df >/dev/null 2>&1; then
    local free_gb
    free_gb="$(df -Pk "${PROJECT_DIR}" | awk 'NR==2 {print int($4/1024/1024)}')"
    if [ -n "${free_gb}" ] && [ "${free_gb}" -lt "${min_free_gb}" ]; then
      echo "Warning: free disk space is ${free_gb} GB. Recommended minimum is ${min_free_gb} GB."
    fi
  fi
}

get_primary_ip() {
  if command -v hostname >/dev/null 2>&1; then
    local ip
    ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
    if [ -n "${ip}" ]; then
      echo "${ip}"
      return 0
    fi
  fi

  echo "127.0.0.1"
}
