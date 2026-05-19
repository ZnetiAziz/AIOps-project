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

## Firewall Appliance Mode

The firewall mode configures the host as a small LAN/WAN gateway with `nftables`.
It is intentionally disabled by default so the appliance does not lock you out
before the correct network interfaces are confirmed.

First identify interfaces:

```bash
ip addr
ip route
```

Then edit `.env`:

```env
FIREWALL_ENABLE=1
FIREWALL_WAN_IFACE=eth0
FIREWALL_LAN_IFACE=eth1
FIREWALL_LAN_CIDR=192.168.10.0/24
FIREWALL_LAN_TCP_PORTS=22,8088,8000,3001,8080,9090,9093
```

Apply the firewall:

```bash
sudo ./appliance/firewall.sh apply
```

Check status:

```bash
./appliance/firewall.sh status
```

Remove only the AIOps firewall tables:

```bash
sudo ./appliance/firewall.sh stop
```

The applied policy:

- allows established traffic;
- allows the LAN CIDR to access selected appliance TCP ports;
- allows LAN to WAN forwarding;
- enables IPv4 forwarding;
- applies NAT masquerading from LAN to WAN;
- drops unsolicited inbound WAN traffic.

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
- Add API-backed nftables rule management.
- Add backup/restore for named Docker volumes.
