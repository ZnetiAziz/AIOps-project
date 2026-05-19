#!/usr/bin/env bash
set -euo pipefail

# ================================================
# DEPLOY — AIOps Appliance Deployment Helper
# ================================================
# Run this on a fresh Ubuntu/Debian server to
# install prerequisites and prepare the project.
#
# Usage: curl ... | bash  (or run directly)
# ================================================

echo "========================================"
echo "  AIOps Appliance — Deploy Helper"
echo "========================================"
echo

# ── Step 0: Check OS ──────────────────────────
if [ ! -f /etc/os-release ]; then
  echo "ERROR: Cannot detect OS. Only Ubuntu/Debian supported."
  exit 1
fi

ID=$(. /etc/os-release && echo "$ID" || echo "unknown")
case "$ID" in
  ubuntu|debian)
    echo "OS detected: $ID"
    ;;
  *)
    echo "WARNING: Unsupported OS '$ID'. Only Ubuntu/Debian tested."
    echo "Press Ctrl+C to abort, or Enter to continue..."
    read -r
    ;;
esac
echo

# ── Step 1: Require root ──────────────────────
if [ "$(id -u)" -ne 0 ]; then
  echo "This script must run as root (or with sudo)."
  echo "Run: sudo $0"
  exit 1
fi

# ── Step 2: Install packages ──────────────────
echo "Updating package lists..."
apt-get update -qq

echo "Installing required packages..."
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  git curl nftables docker.io docker-compose-plugin \
  >/dev/null 2>&1

echo "Enabling Docker service..."
systemctl enable --now docker

echo "Docker version: $(docker --version)"
echo "Compose version: $(docker compose version)"
echo

# ── Step 3: Check hardware ────────────────────
TOTAL_RAM_MB=$(free -m | awk '/Mem:/ {print $2}')
FREE_DISK_GB=$(df -Pk / | awk 'NR==2 {print int($4/1024/1024)}')
CPU_CORES=$(nproc)

echo "Hardware:"
echo "  CPU cores : $CPU_CORES (min: 4)"
echo "  RAM       : ${TOTAL_RAM_MB} MB (min: 8192)"
echo "  Free disk : ${FREE_DISK_GB} GB (min: 15)"
echo

WARNINGS=0
if [ "$CPU_CORES" -lt 4 ]; then
  echo "  ⚠ WARNING: Less than 4 CPU cores. Ollama will be slow."
  WARNINGS=1
fi
if [ "$TOTAL_RAM_MB" -lt 8192 ]; then
  echo "  ⚠ WARNING: Less than 8 GB RAM. Consider running Ollama on another machine."
  WARNINGS=1
fi
if [ "$FREE_DISK_GB" -lt 15 ]; then
  echo "  ⚠ WARNING: Less than 15 GB free disk."
  WARNINGS=1
fi
if [ "$WARNINGS" -eq 1 ]; then
  echo
  echo "Press Ctrl+C to abort, or Enter to continue..."
  read -r
fi
echo

# ── Step 4: Network interfaces ────────────────
echo "Network interfaces:"
echo "---"
ip -br addr show | grep -v LOOPBACK || true
echo "---"
echo
echo "Default route:"
ip route show default || echo "  (none)"
echo

echo "Identify your interfaces:"
echo "  - WAN iface: connected to internet/router"
echo "  - LAN iface: connected to internal switch"
echo
echo "Example:"
echo "  eth0 = WAN"
echo "  eth1 = LAN"
echo

# ── Step 5: Clone or detect project ───────────
PROJECT_DIR="/opt/aiops-project"

if [ -d "$PROJECT_DIR" ]; then
  echo "Project already exists at $PROJECT_DIR"
  echo "Pulling latest changes..."
  cd "$PROJECT_DIR"
  git pull --ff-only 2>/dev/null || echo "  (not a git repo or no remote — skipping pull)"
else
  echo "Where is the project?"
  echo "  1) Clone from a git remote"
  echo "  2) Already copied to this server"
  read -r -p "Choice [1/2]: " choice
  case "$choice" in
    1)
      read -r -p "Git repository URL: " repo_url
      git clone "$repo_url" "$PROJECT_DIR"
      cd "$PROJECT_DIR"
      ;;
    2)
      read -r -p "Path to project directory: " user_path
      PROJECT_DIR="$(realpath "$user_path")"
      if [ ! -f "$PROJECT_DIR/docker/docker-compose.yml" ]; then
        echo "ERROR: $PROJECT_DIR does not look like the AIOps project."
        echo "Missing: docker/docker-compose.yml"
        exit 1
      fi
      cd "$PROJECT_DIR"
      ;;
    *)
      echo "Invalid choice."
      exit 1
      ;;
  esac
fi

# ── Step 6: Configure .env ────────────────────
if [ ! -f "$PROJECT_DIR/.env" ]; then
  cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
  echo "Created .env from .env.example"
fi

echo
echo "Configure .env:"
nano "$PROJECT_DIR/.env" 2>/dev/null || vi "$PROJECT_DIR/.env"
echo

# ── Step 7: Summary ───────────────────────────
echo "========================================"
echo "  Next steps:"
echo "========================================"
echo
echo "1. Start AIOps:"
echo "   cd $PROJECT_DIR"
echo "   ./appliance/start.sh"
echo
echo "2. Check status:"
echo "   ./appliance/status.sh"
echo
echo "3. Open from LAN:"
echo "   http://SERVER_IP:8088  (Dashboard)"
echo "   http://SERVER_IP:3001  (Grafana)"
echo "   http://SERVER_IP:8000/docs (API)"
echo
echo "4. Apply firewall (ONLY after verifying access):"
echo "   ./appliance/firewall.sh apply"
echo
echo "5. Install as boot service:"
echo "   ./appliance/install.sh"
