#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

compose_render="$(mktemp)"
render_dir="$(mktemp -d)"
validation_project="trpc-agent-service-validation-${BASHPID:-$$}"
export COMPOSE_PROJECT_NAME="$validation_project"

cleanup() {
  docker compose --env-file .env.example down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -f "$compose_render"
  rm -rf "$render_dir"
}
trap cleanup EXIT

docker compose --env-file .env.example config > "$compose_render"

for required in \
  'postgres:' \
  'redis:' \
  'qdrant:' \
  'gateway:' \
  'channel:' \
  'worker-1:' \
  'worker-2:' \
  'healthcheck:' \
  'condition: service_healthy' \
  'stop_grace_period: 45s'; do
  if ! grep -Fq "$required" "$compose_render"; then
    echo "deployment validation failed: Compose is missing $required" >&2
    exit 1
  fi
done

for service in postgres redis qdrant gateway channel worker-1 worker-2; do
  if ! awk -v service="$service" '
    $0 == "  " service ":" { in_service=1; next }
    in_service && /^  [^[:space:]][^:]*:/ { in_service=0 }
    in_service && /^    healthcheck:/ { found=1 }
    END { exit found ? 0 : 1 }
  ' "$compose_render"; then
    echo "deployment validation failed: Compose service $service has no healthcheck" >&2
    exit 1
  fi
done

targets=(
  deploy/kubernetes/base
  deploy/kubernetes/overlays/dev
  deploy/kubernetes/overlays/staging
  deploy/kubernetes/overlays/production
)

validate_kubernetes_yaml() {
  local rendered="$1"
  if command -v kubeconform >/dev/null 2>&1; then
    kubeconform -strict -summary "$rendered"
    return
  fi

  # kubectl kustomize has already parsed the source YAML. This offline
  # fallback adds an explicit API version/kind allow-list without requiring a
  # live Kubernetes API server for OpenAPI discovery.
  awk '
    function valid(api, kind) {
      return (api == "v1" && (kind == "ConfigMap" || kind == "Service")) ||
        (api == "apps/v1" && kind == "Deployment") ||
        (api == "autoscaling/v2" && kind == "HorizontalPodAutoscaler") ||
        (api == "policy/v1" && kind == "PodDisruptionBudget")
    }
    /^apiVersion:/ { api=$2 }
    /^kind:/ {
      kind=$2
      if (!valid(api, kind)) {
        print "unsupported Kubernetes apiVersion/kind: " api "/" kind > "/dev/stderr"
        failed=1
      }
    }
    END { exit failed }
  ' "$rendered"
}

for target in "${targets[@]}"; do
  name="${target//\//-}"
  rendered="$render_dir/$name.yaml"
  kubectl kustomize "$target" > "$rendered"
  validate_kubernetes_yaml "$rendered"
done

kubectl kustomize deploy/kubernetes > "$render_dir/root.yaml"
validate_kubernetes_yaml "$render_dir/root.yaml"

production="$render_dir/deploy-kubernetes-overlays-production.yaml"

for required in \
  'kind: Deployment' \
  'name: trpc-agent-service-gateway' \
  'name: trpc-agent-service-channel' \
  'name: trpc-agent-service-worker' \
  'kind: Service' \
  'kind: HorizontalPodAutoscaler' \
  'kind: PodDisruptionBudget' \
  'path: /readyz' \
  'path: /healthz' \
  'containerPort: 8080' \
  'terminationGracePeriodSeconds: 45'; do
  if ! grep -Fq "$required" "$render_dir/deploy-kubernetes-base.yaml"; then
    echo "deployment validation failed: missing $required" >&2
    exit 1
  fi
done

for required in \
  'name: trpc-agent-service-config' \
  'name: trpc-agent-service-secrets' \
  'key: TRPC_AGENT_SERVICE_POSTGRES_DSN' \
  'key: TRPC_AGENT_SERVICE_REDIS_URL' \
  'imagePullPolicy: Always' \
  'path: /readyz' \
  'path: /healthz'; do
  if ! grep -Fq "$required" "$production"; then
    echo "deployment validation failed: production render is missing $required" >&2
    exit 1
  fi
done

if grep -Eiq 'image:.*(:latest|:dev|REPLACE_ME|CHANGE_ME|\$\{|example\.invalid)' "$production"; then
  echo 'deployment validation failed: production image is not a fixed release image' >&2
  exit 1
fi

if [ "$(grep -Ec 'image: [^[:space:]]+@sha256:[0-9a-f]{64}$' "$production")" -ne 3 ]; then
  echo 'deployment validation failed: production workloads must use immutable image digests' >&2
  exit 1
fi

if [ "$(grep -Fc 'imagePullPolicy: Always' "$production")" -ne 3 ]; then
  echo 'deployment validation failed: production workloads must always pull the fixed image' >&2
  exit 1
fi

if [ "$(grep -Fc 'name: TRPC_AGENT_SERVICE_OPERATOR_TOKEN' "$production")" -ne 2 ] || \
  [ "$(grep -Fc 'name: TRPC_AGENT_SERVICE_AUDITOR_TOKEN' "$production")" -ne 2 ]; then
  echo 'deployment validation failed: control-plane role tokens must be cleared outside Gateway' >&2
  exit 1
fi

if ! grep -Fq 'name: trpc-agent-service-channel' "$production" || \
  ! grep -Fq 'type: Recreate' "$production" || \
  ! grep -Fq 'replicas: 1' "$production"; then
  echo 'deployment validation failed: channel deployment must remain a single Recreate owner' >&2
  exit 1
fi

if grep -Eq 'redis://redis:6379|qdrant:6334|otel-collector:4317' "$production"; then
  echo 'deployment validation failed: production render has an in-cluster dependency default' >&2
  exit 1
fi

if ! grep -Fq 'targetPort: http' "$render_dir/deploy-kubernetes-base.yaml"; then
  echo 'deployment validation failed: Service targetPort is not the named HTTP port' >&2
  exit 1
fi

if ! grep -Fq 'TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT: 60s' \
  deploy/kubernetes/overlays/production/configmap-patch.yaml || \
  ! grep -Fq 'terminationGracePeriodSeconds: 75' "$production"; then
  echo 'deployment validation failed: production shutdown grace is not longer than app timeout' >&2
  exit 1
fi

if grep -Eiq 'password|admin[_-]?token|api[_-]?key|authorization|bearer' \
  deploy/kubernetes/base/configmap.yaml \
  deploy/kubernetes/overlays/staging/configmap-patch.yaml \
  deploy/kubernetes/overlays/production/configmap-patch.yaml; then
  echo 'deployment validation failed: sensitive value found in ConfigMap' >&2
  exit 1
fi

if grep -Eiq 'kind:[[:space:]]*Secret' "$production"; then
  echo 'deployment validation failed: production render must not generate a Secret' >&2
  exit 1
fi

tracked_secrets="$(git grep -n -E '^kind:[[:space:]]*Secret' -- deploy/kubernetes 2>/dev/null || true)"
tracked_secrets="$(printf '%s\n' "$tracked_secrets" | grep -v 'deploy/kubernetes/secret\.example\.yaml' || true)"
if [ -n "$tracked_secrets" ]; then
  echo 'deployment validation failed: real Kubernetes Secret is tracked in Git' >&2
  printf '%s\n' "$tracked_secrets" >&2
  exit 1
fi

if ! grep -Fq 'EXAMPLE ONLY' deploy/kubernetes/secret.example.yaml || \
  ! grep -Fq 'replace-me' deploy/kubernetes/secret.example.yaml; then
  echo 'deployment validation failed: Secret example is missing fake-value marker' >&2
  exit 1
fi

MSYS_NO_PATHCONV=1 docker compose --env-file .env.example run --rm --no-deps \
  --entrypoint /otelcol-contrib otel-collector \
  validate --config=file:/etc/otelcol/config.yaml

docker build --file Dockerfile --tag trpc-agent-service:deployment-validation .
docker build --file admin-ui/Dockerfile --tag trpc-agent-service-admin-ui:deployment-validation admin-ui
