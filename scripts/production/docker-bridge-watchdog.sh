#!/bin/bash
# POST-P2 VM2 Docker bridge watchdog: fixes docker0 IPv4 address loss and
# monitors VM1 PG reachability. Deployed as a systemd timer (2-minute
# interval) on VM2. Referenced from the POST-P2 VM1/VM2 HA wiring runbook.
set -euo pipefail
LOG_TAG="docker-bridge-watchdog"
if ! ip addr show docker0 | grep -q '172.17.0.1'; then
    logger -t "$LOG_TAG" "docker0 IPv4 missing; restoring 172.17.0.1/16"
    ip addr add 172.17.0.1/16 dev docker0 2>/dev/null || true
fi
if ! nc -z -w 3 192.168.1.6 5432 2>/dev/null; then
    logger -t "$LOG_TAG" "VM1:5432 unreachable; alerting operator"
    exit 1
fi
