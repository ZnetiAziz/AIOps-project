#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/lib.sh"

ACTION="${1:-status}"
FILTER_TABLE="aiops_filter"
NAT_TABLE="aiops_nat"

usage() {
  cat <<EOF
Usage: $0 <apply|status|stop>

Commands:
  apply   Apply the AIOps LAN/WAN nftables firewall
  status  Show firewall configuration and active rules
  stop    Remove only the AIOps nftables tables

Required .env values:
  FIREWALL_ENABLE=1
  FIREWALL_WAN_IFACE=eth0
  FIREWALL_LAN_IFACE=eth1
  FIREWALL_LAN_CIDR=192.168.10.0/24
EOF
}

load_firewall_env() {
  ensure_env_file
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a

  FIREWALL_ENABLE="${FIREWALL_ENABLE:-0}"
  FIREWALL_WAN_IFACE="${FIREWALL_WAN_IFACE:-eth0}"
  FIREWALL_LAN_IFACE="${FIREWALL_LAN_IFACE:-eth1}"
  FIREWALL_LAN_CIDR="${FIREWALL_LAN_CIDR:-192.168.10.0/24}"
  FIREWALL_LAN_TCP_PORTS="${FIREWALL_LAN_TCP_PORTS:-22,8088,8000,3001,8080,9090,9093}"
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "This command must run as root. Use sudo."
    exit 1
  fi
}

require_firewall_tools() {
  require_command nft
  require_command sysctl
  require_command ip
}

validate_iface() {
  local iface="$1"
  local name="$2"
  if ! ip link show "${iface}" >/dev/null 2>&1; then
    echo "${name} interface does not exist: ${iface}"
    echo "Check interfaces with: ip addr"
    exit 1
  fi
}

validate_enabled() {
  if [ "${FIREWALL_ENABLE}" != "1" ]; then
    echo "Firewall is disabled. Set FIREWALL_ENABLE=1 in ${ENV_FILE} before applying."
    exit 1
  fi
}

apply_firewall() {
  load_firewall_env
  validate_enabled
  require_root
  require_firewall_tools
  validate_iface "${FIREWALL_WAN_IFACE}" "WAN"
  validate_iface "${FIREWALL_LAN_IFACE}" "LAN"

  local rules_file
  rules_file="$(mktemp)"
  trap 'rm -f "${rules_file}"' EXIT

  nft delete table inet "${FILTER_TABLE}" >/dev/null 2>&1 || true
  nft delete table ip "${NAT_TABLE}" >/dev/null 2>&1 || true

  cat >"${rules_file}" <<EOF
table inet ${FILTER_TABLE} {
  chain input {
    type filter hook input priority 0; policy drop;

    iifname "lo" accept
    ct state established,related accept
    ct state invalid drop

    ip protocol icmp accept
    ip6 nexthdr icmpv6 accept

    iifname "${FIREWALL_LAN_IFACE}" ip saddr ${FIREWALL_LAN_CIDR} tcp dport { ${FIREWALL_LAN_TCP_PORTS} } accept
    iifname "${FIREWALL_LAN_IFACE}" ip saddr ${FIREWALL_LAN_CIDR} udp dport { 53, 67, 68 } accept
  }

  chain forward {
    type filter hook forward priority 0; policy drop;

    ct state established,related accept
    ct state invalid drop

    iifname "${FIREWALL_LAN_IFACE}" oifname "${FIREWALL_WAN_IFACE}" ip saddr ${FIREWALL_LAN_CIDR} accept
  }

  chain output {
    type filter hook output priority 0; policy accept;
  }
}

table ip ${NAT_TABLE} {
  chain postrouting {
    type nat hook postrouting priority 100; policy accept;

    oifname "${FIREWALL_WAN_IFACE}" ip saddr ${FIREWALL_LAN_CIDR} masquerade
  }
}
EOF

  nft -c -f "${rules_file}"
  nft -f "${rules_file}"
  sysctl -w net.ipv4.ip_forward=1 >/dev/null

  if [ -d /etc/sysctl.d ]; then
    printf 'net.ipv4.ip_forward=1\n' >/etc/sysctl.d/99-aiops-firewall.conf
  fi

  echo "AIOps firewall applied."
  echo "WAN: ${FIREWALL_WAN_IFACE}"
  echo "LAN: ${FIREWALL_LAN_IFACE} (${FIREWALL_LAN_CIDR})"
  echo "LAN appliance TCP ports: ${FIREWALL_LAN_TCP_PORTS}"
}

stop_firewall() {
  require_root
  require_firewall_tools

  nft delete table inet "${FILTER_TABLE}" >/dev/null 2>&1 || true
  nft delete table ip "${NAT_TABLE}" >/dev/null 2>&1 || true
  echo "AIOps firewall tables removed."
}

show_status() {
  load_firewall_env
  require_firewall_tools

  echo "Firewall env:"
  echo "  enabled: ${FIREWALL_ENABLE}"
  echo "  WAN:     ${FIREWALL_WAN_IFACE}"
  echo "  LAN:     ${FIREWALL_LAN_IFACE}"
  echo "  LAN CIDR:${FIREWALL_LAN_CIDR}"
  echo "  ports:   ${FIREWALL_LAN_TCP_PORTS}"
  echo
  echo "IPv4 forwarding:"
  sysctl net.ipv4.ip_forward || true
  echo
  echo "AIOps nftables rules:"
  nft list table inet "${FILTER_TABLE}" 2>/dev/null || echo "  inet ${FILTER_TABLE}: not installed"
  nft list table ip "${NAT_TABLE}" 2>/dev/null || echo "  ip ${NAT_TABLE}: not installed"
}

case "${ACTION}" in
  apply)
    apply_firewall
    ;;
  status)
    show_status
    ;;
  stop)
    stop_firewall
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
