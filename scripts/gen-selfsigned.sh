#!/usr/bin/env bash
# Generate a self-signed certificate for the production edge.
#
#   bash scripts/gen-selfsigned.sh [hostname]
#
# This is for a staging/demo deployment and for verifying that the TLS edge
# actually terminates and routes. A real deployment uses a certificate from its
# CA (or an ACME client) dropped into deployments/prod/certs/tls.crt|tls.key —
# the edge config does not care where they came from.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="$ROOT/deployments/prod/certs"
HOST="${1:-localhost}"

mkdir -p "$CERT_DIR"

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required" >&2
  exit 1
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$CERT_DIR/tls.key" -out "$CERT_DIR/tls.crt" \
  -subj "/CN=${HOST}" \
  -addext "subjectAltName=DNS:${HOST},DNS:localhost,IP:127.0.0.1" 2>/dev/null

chmod 600 "$CERT_DIR/tls.key"
echo "wrote $CERT_DIR/tls.crt and tls.key (CN=${HOST}, valid 365 days, self-signed)"
echo "note: self-signed, so clients must trust it explicitly or skip verification (curl -k)"
