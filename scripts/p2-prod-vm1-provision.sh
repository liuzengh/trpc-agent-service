#!/bin/bash
# P2 POST-P2 WS-3A: VM1 production-shaped stack provisioning (idempotent).
# Run ON VM1 as a sudo-capable user. Creates deploy dirs, generates random
# secrets (0600), writes the compose file and the OTel config, then starts
# the infrastructure services (pg-primary/redis/otel). The app service is
# started separately after the control node loads the release image and the
# migration/runtime roles are provisioned.
#
# Resources carry the prefix p2prod-* so ownership is auditable.
set -euo pipefail

DEPLOY_DIR="${P2PROD_DEPLOY_DIR:-/opt/p2prod}"
COMPOSE_FILE="$DEPLOY_DIR/vm1.compose.yml"
OTEL_CONFIG="$DEPLOY_DIR/otel-config.yaml"
SECRET_DIR="$DEPLOY_DIR/secrets"

echo "[1/5] deploy directories"
sudo mkdir -p "$DEPLOY_DIR" "$SECRET_DIR"
sudo chown "$(id -u):$(id -g)" "$DEPLOY_DIR"
chmod 700 "$SECRET_DIR"

echo "[2/5] secrets (random, 0600; existing files are kept)"
gen_secret() {
  local f="$SECRET_DIR/$1"
  if [ -f "$f" ]; then echo "  keep $1"; return; fi
  head -c 32 /dev/urandom | base64 | tr -d '\n/+=' | head -c 32 > "$f"
  chmod 600 "$f"
  echo "  generated $1"
}
gen_secret pg_postgres_password
gen_secret pg_repl_password
gen_secret pg_runtime_password
gen_secret redis_password

# Redis config generated from the secret at provision time (no plaintext in
# the compose file; the official redis image runs it via its entrypoint).
cat > "$DEPLOY_DIR/redis.conf" <<RC
requirepass $(cat "$SECRET_DIR/redis_password")
save 60 1
dir /data
RC
chmod 600 "$DEPLOY_DIR/redis.conf"

# PostgreSQL replication bootstrap: the init script (first boot only) creates
# the replication role and prepares pg_hba for the future VM2 standby.
sudo mkdir -p "$DEPLOY_DIR/pg-init"
sudo tee "$DEPLOY_DIR/pg-init/01-replication.sh" > /dev/null <<'PGINIT'
#!/bin/bash
set -euo pipefail
REPL_PASSWORD="$(cat /run/secrets/pg_repl_password)"
echo "host replication repl all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U postgres -d trpc_agent <<SQL
CREATE ROLE repl WITH REPLICATION LOGIN PASSWORD '${REPL_PASSWORD}';
SQL
PGINIT
sudo chmod 700 "$DEPLOY_DIR/pg-init" && sudo chmod 600 "$DEPLOY_DIR/pg-init/01-replication.sh"

echo "[3/5] compose file"
if [ -f "$COMPOSE_FILE" ]; then
  echo "  keep $COMPOSE_FILE"
else
  cat > "$COMPOSE_FILE" <<'COMPOSE'
# POST-P2 WS-3A: VM1 production-shaped stack (single active node, HA-ready).
# PG runs as the repmgr PRIMARY ready for a VM2 standby to join later; Redis
# is an expendable rate-limit budget store (authority stays in PostgreSQL);
# telemetry export stays no-export until TLS/IAM are provisioned (P1-07
# contract). All credentials flow through Docker secret files (0600), never
# through environment literals.
name: p2prod
x-app-env: &app-env
  DATABASE_SCHEMA: public
  HTTP_ADDR: 0.0.0.0:8080
  DEFAULT_TENANT: p2-prod
  DEFAULT_AGENT_APP_ID: p2-prod-agent
  MODEL_PROVIDER: runner
  MODEL_BASE_URL: http://127.0.0.1:9
  MODEL_NAME: p2-prod-model
  MODEL_API_KEY: p2-prod-placeholder-not-a-secret
  MODEL_CONFIG_VERSION: "1"
  ASYNC_OWNER_ID: p2-prod-owner
  BOOTSTRAP_OBJECT_BACKEND: none
  BOOTSTRAP_TENANT_ID: p2-prod
  BOOTSTRAP_TENANT_NAME: p2-prod
  BOOTSTRAP_AGENT_APP_ID: p2-prod-agent
  BOOTSTRAP_AGENT_NAME: p2-prod-agent
  BOOTSTRAP_AGENT_VERSION: "1"
  BOOTSTRAP_LARK_ENABLED: "false"
  BOOTSTRAP_TELEGRAM_ENABLED: "false"
  BOOTSTRAP_LARK_BINDING_ID: prod-lark
  BOOTSTRAP_TELEGRAM_BINDING_ID: prod-tg
  BOOTSTRAP_LARK_EXTERNAL_APP_ID: prod-lark-ext
  BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID: prod-tg-ext
  BOOTSTRAP_LARK_APP_ID: prod-lark-app
  BOOTSTRAP_LARK_APP_SECRET_REF: "env://P2PROD_LARK_SECRET"
  BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF: "env://P2PROD_TG_TOKEN"
  BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF: "env://P2PROD_TG_HOOK"
  BOOTSTRAP_LARK_VERIFY_TOKEN_REF: "env://P2PROD_LARK_VERIFY"
  BOOTSTRAP_LARK_ENCRYPT_KEY_REF: "env://P2PROD_LARK_ENCRYPT"
  BOOTSTRAP_LARK_RECEIVER_ID_TYPE: open_id
  OTEL_EXPORTER_OTLP_ENDPOINT: ""
  TELEMETRY_MODE: ""
  TELEMETRY_INSECURE_LOCAL: ""
  RATE_LIMIT_WINDOW: 1m
  RATE_LIMIT_TENANT_LIMIT: "600"
  RATE_LIMIT_BINDING_LIMIT: "300"
  RATE_LIMIT_CHAT_LIMIT: "120"
  INGRESS_MAX_INFLIGHT: "64"
  INGRESS_MAX_INFLIGHT_PER_TENANT: "32"
  INGRESS_MAX_INFLIGHT_PER_BINDING: "16"
  WORKER_CONCURRENCY: "4"
  DISPATCHER_CONCURRENCY: "1"
  DISPATCHER_CLAIM_BATCH_SIZE: "100"
services:
  vm1-pg:
    image: postgres:16-alpine
    container_name: p2prod-vm1-pg
    restart: unless-stopped
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/pg_postgres_password
      POSTGRES_DB: trpc_agent
    secrets:
      - pg_postgres_password
      - pg_repl_password
    volumes:
      - pgdata:/var/lib/postgresql/data
      - /opt/p2prod/pg-init:/docker-entrypoint-initdb.d:ro
    command:
      - postgres
      - -c
      - wal_level=replica
      - -c
      - max_wal_senders=10
      - -c
      - max_replication_slots=10
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d trpc_agent"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s
    ports:
      - "192.168.1.6:5432:5432"
  redis:
    image: redis:7.4-alpine
    container_name: p2prod-redis
    restart: unless-stopped
    command: ["redis-server", "/usr/local/etc/redis/redis.conf"]
    volumes:
      - redisdata:/data
      - /opt/p2prod/redis.conf:/usr/local/etc/redis/redis.conf:ro
    healthcheck:
      test: ["CMD-SHELL", "redis-cli -a $(cat /run/secrets/redis_password) ping | grep PONG"]
      interval: 10s
      timeout: 5s
      retries: 12
  otel:
    image: otel/opentelemetry-collector-contrib:0.111.0
    container_name: p2prod-otel
    restart: unless-stopped
    command: ["--config=/etc/otelcol/config.yaml"]
    volumes:
      - /opt/p2prod/otel-config.yaml:/etc/otelcol/config.yaml:ro
  app:
    image: app:release
    container_name: p2prod-app
    restart: unless-stopped
    user: "0:0"
    entrypoint: ["sh", "-c"]
    command:
      - |
        set -eu
        export DATABASE_URL="postgres://postgres:$(cat /run/secrets/pg_postgres_password)@vm1-pg:5432/trpc_agent?sslmode=disable"
        export DATABASE_RUNTIME_URL="postgres://trpc_runtime:$(cat /run/secrets/pg_runtime_password)@vm1-pg:5432/trpc_agent?sslmode=disable"
        export RATE_LIMIT_REDIS_URL="redis://:$(cat /run/secrets/redis_password)@redis:6379/0"
        exec su-exec app /usr/local/bin/trpc-service
    secrets:
      - pg_postgres_password
      - pg_runtime_password
      - redis_password
    environment: *app-env
    depends_on:
      vm1-pg:
        condition: service_healthy
      redis:
        condition: service_started
    ports:
      - "192.168.1.6:8080:8080"
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s
secrets:
  pg_postgres_password:
    file: /opt/p2prod/secrets/pg_postgres_password
  pg_repl_password:
    file: /opt/p2prod/secrets/pg_repl_password
  pg_runtime_password:
    file: /opt/p2prod/secrets/pg_runtime_password
  redis_password:
    file: /opt/p2prod/secrets/redis_password
volumes:
  pgdata:
    name: p2prod-pgdata
  redisdata:
    name: p2prod-redisdata

COMPOSE
  echo "  wrote $COMPOSE_FILE"
fi

echo "[4/5] otel config (P1-09 local semantics; export disabled until TLS/IAM)"
if [ -f "$OTEL_CONFIG" ]; then
  echo "  keep $OTEL_CONFIG"
else
  cat > "$OTEL_CONFIG" <<'OTEL'
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
processors:
  batch:
exporters:
  debug:
    verbosity: basic
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
OTEL
  echo "  wrote $OTEL_CONFIG"
fi

echo "[5/5] start infrastructure (pg-primary, redis, otel)"
sudo docker compose -f "$COMPOSE_FILE" up -d vm1-pg redis otel
sudo docker compose -f "$COMPOSE_FILE" ps

echo "provision done: infra up; app starts after image load + migration"
