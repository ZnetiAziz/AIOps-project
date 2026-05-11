# AIOps Appliance

This folder turns the project into a first appliance-style deployment for a mini-PC, firewall box, or local server.

## Target V1

- Boot Linux.
- Start Docker automatically.
- Start the AIOps stack automatically.
- Serve the dashboard on the LAN.
- Persist data in Docker volumes.
- Keep all configuration in `.env`.

## Hardware baseline

Recommended:

- x86_64 mini-PC or server.
- 4 CPU cores minimum.
- 16 GB RAM recommended if Ollama/Mistral runs locally.
- 64 GB SSD minimum.
- Debian or Ubuntu Server.

For small firewall hardware, keep Ollama on another machine and set `OLLAMA_URL` in `.env`.

## Manual Start

From the project root:

```bash
cp .env.example .env
./appliance/start.sh
```

Open:

```text
http://DEVICE_IP:8088
```

## Install As A Boot Service

```bash
./appliance/install.sh
```

The installer checks that Docker is reachable and prints warnings if the machine looks undersized for the full stack.

Useful commands:

```bash
sudo systemctl status aiops-appliance
sudo systemctl restart aiops-appliance
./appliance/status.sh
./appliance/logs.sh aiops-api
./appliance/backup.sh
```

## Ports

- Dashboard: `8088`
- AIOps API: `8000`
- Grafana: `3001`
- Zabbix: `8080`
- Prometheus: `9090`
- Alertmanager: `9093`
- Ollama: `11434`

## Next Appliance Work

- Add firewall traffic dashboards from network interfaces.
- Add nftables rule management.
- Add backup/restore for named Docker volumes.
