package base

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readManifest(t *testing.T, name ...string) string {
	t.Helper()
	path := filepath.Join(name...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestWorkerAndChannelClearControlPlaneTokens(t *testing.T) {
	controlPlaneTokens := []string{
		"TRPC_AGENT_SERVICE_ADMIN_TOKEN",
		"TRPC_AGENT_SERVICE_OPERATOR_TOKEN",
		"TRPC_AGENT_SERVICE_AUDITOR_TOKEN",
	}
	for _, name := range []string{"worker-deployment.yaml", "channel-deployment.yaml"} {
		manifest := readManifest(t, name)
		if strings.Contains(manifest, "trpc-agent-service-admin-secrets") {
			t.Errorf("%s mounts the admin secret", name)
		}
		for _, token := range controlPlaneTokens {
			declaration := "            - name: " + token + "\n              value: \"\""
			if !strings.Contains(manifest, declaration) {
				t.Errorf("%s does not clear %s", name, token)
			}
		}
	}
}

func TestGatewayUsesSeparateAdminSecret(t *testing.T) {
	manifest := readManifest(t, "gateway-deployment.yaml")
	if !strings.Contains(manifest, "name: trpc-agent-service-admin-secrets") ||
		!strings.Contains(manifest, "key: TRPC_AGENT_SERVICE_ADMIN_TOKEN") {
		t.Fatal("gateway does not reference the control-plane secret")
	}
	secretExample := readManifest(t, "..", "secret.example.yaml")
	adminStart := strings.Index(secretExample, "name: trpc-agent-service-admin-secrets")
	if adminStart < 0 || strings.Contains(secretExample[:adminStart], "TRPC_AGENT_SERVICE_ADMIN_TOKEN") {
		t.Fatal("admin token remains in the shared Secret example")
	}
}

func TestComposePassesSecretBackendConfigurationToAllAppRoles(t *testing.T) {
	compose := readManifest(t, "..", "..", "..", "compose.yaml")
	if got := strings.Count(compose, "      TRPC_AGENT_SERVICE_SECRET_BACKEND:"); got != 4 {
		t.Fatalf("secret backend configuration appears %d times, want four app containers", got)
	}
	for _, key := range []string{
		"TRPC_AGENT_SERVICE_SECRET_DIR:",
		"TRPC_AGENT_SERVICE_VAULT_ADDR:",
		"TRPC_AGENT_SERVICE_VAULT_MOUNT:",
		"TRPC_AGENT_SERVICE_VAULT_TOKEN:",
		"TRPC_AGENT_SERVICE_VAULT_NAMESPACE:",
	} {
		if got := strings.Count(compose, "      "+key); got != 4 {
			t.Errorf("%s appears %d times, want four app containers", key, got)
		}
	}
}

func TestWorkerHPAUsesQueueBacklogBusinessMetric(t *testing.T) {
	manifest := readManifest(t, "hpa.yaml")
	workerStart := strings.Index(manifest, "name: trpc-agent-service-worker")
	if workerStart < 0 {
		t.Fatal("worker HPA is missing")
	}
	worker := manifest[workerStart:]
	if !strings.Contains(worker, "type: External") ||
		!strings.Contains(worker, "name: trpc_agent_service_queue_backlog") ||
		!strings.Contains(worker, "value: \"10\"") {
		t.Fatal("worker HPA does not contain the queue backlog external metric")
	}
	if strings.Contains(manifest[:workerStart], "trpc_agent_service_queue_backlog") {
		t.Fatal("gateway HPA must not scale on the worker queue backlog metric")
	}
}
