# AIOps Appliance — Deployment Guide

## Prerequisites

- **OS**: Ubuntu Server 22.04+ or Debian 12+
- **CPU**: 4 cores minimum (8 recommended for Ollama)
- **RAM**: 8 GB minimum (16 GB recommended)
- **Disk**: 64 GB SSD minimum
- **Network**: 2 Ethernet ports for firewall mode (1 port works for monitoring only)

## Step-by-Step

### 1. Install Linux

Install Ubuntu Server or Debian on your appliance hardware.

### 2. Connect Network Cables

For firewall mode, you need 2 NICs:

| Port | Role | Example |
|------|------|---------|
| eth0 | WAN → router/internet | `192.168.1.x` |
| eth1 | LAN → internal switch | `10.0.0.x` |

> If you only have 1 NIC, you can still run the monitoring stack.
> Just skip the firewall step.

### 3. Install Required Packages

```bash
sudo apt update
sudo apt install -y git curl nftables docker.io docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
```

Log out and back in, then verify:

```bash
docker --version
docker compose version
```

### 4. Deploy the Project

**Option A — Automated (recommended):**

```bash
sudo bash appliance/deploy.sh
```

This script handles package installation, hardware checks, network detection,
project setup, and `.env` configuration interactively.

**Option B — Manual:**

```bash
# Clone or copy the project
git clone YOUR_REPO_URL aiops-project
cd aiops-project

# Create .env
cp .env.example .env
nano .env
```

### 5. Detect Network Interfaces

```bash
ip addr
ip route
```

Send the output before applying firewall rules. You need to identify:

```env
FIREWALL_WAN_IFACE=eth0
FIREWALL_LAN_IFACE=eth1
FIREWALL_LAN_CIDR=192.168.10.0/24
```

### 6. Configure .env

```bash
cp .env.example .env
nano .env
```

Minimum settings:

```env
DASHBOARD_PORT=8088
ALLOWED_ORIGINS=http://localhost:8088,http://SERVER_IP:8088
FIREWALL_ENABLE=0        # Keep 0 until step 8
```

If using firewall mode:

```env
FIREWALL_ENABLE=1
FIREWALL_WAN_IFACE=eth0
FIREWALL_LAN_IFACE=eth1
FIREWALL_LAN_CIDR=192.168.10.0/24
```

### 7. Start AIOps (Before Firewall)

```bash
./appliance/start.sh
./appliance/status.sh
```

From a LAN client, verify access:

```
http://SERVER_IP:8088     # Dashboard
http://SERVER_IP:3001     # Grafana (admin / admin123)
http://SERVER_IP:8000/docs  # API Swagger
```

### 8. Apply Firewall (Only After Access Works)

```bash
sudo ./appliance/firewall.sh apply
sudo ./appliance/firewall.sh status
```

Verify rules:

```bash
sudo nft list ruleset
```

### 9. Test

From a LAN client:

```bash
ping 8.8.8.8
curl http://SERVER_IP:8088
```

On the server:

```bash
./appliance/status.sh
./appliance/firewall.sh status
```

### 10. Install as Boot Service (Optional)

```bash
sudo ./appliance/install.sh
```

Verify:

```bash
sudo systemctl status aiops-appliance
sudo systemctl restart aiops-appliance
```

## Useful Commands

| Command | Description |
|---------|-------------|
| `./appliance/start.sh` | Start the full stack |
| `./appliance/stop.sh` | Stop all containers |
| `./appliance/status.sh` | Show container status |
| `./appliance/logs.sh <service>` | View logs (e.g. `aiops-api`) |
| `./appliance/backup.sh` | Backup data volumes |
| `./appliance/firewall.sh apply` | Apply nftables rules |
| `./appliance/firewall.sh status` | Show firewall state |
| `./appliance/firewall.sh stop` | Remove AIOps firewall rules |

## Ports

| Service | Port | URL |
|---------|------|-----|
| Dashboard | 8088 | `http://IP:8088` |
| AIOps API | 8000 | `http://IP:8000/docs` |
| Grafana | 3001 | `http://IP:3001` |
| Zabbix | 8080 | `http://IP:8080` |
| Prometheus | 9090 | `http://IP:9090` |
| Alertmanager | 9093 | `http://IP:9093` |
| Ollama | 11434 | `http://IP:11434` |
