package scripts

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestEmbeddingCheckScriptOnlyRunsDedicatedDiagnostic(t *testing.T) {
	raw, err := os.ReadFile("../check-embedding.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, forbidden := range []string{"source .env", "docker compose", "start-real.sh", "stop.sh", "trpc-migrate", "setWebhook", "sendMessage"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("script must not operate live services: %s", forbidden)
		}
	}
	for _, required := range []string{"TRPC_AGENT_ENV_FILE", "-u TRPC_AGENT_EMBEDDING_MODEL", "-u TRPC_AGENT_EMBEDDING_BASE_URL", "-u TRPC_AGENT_EMBEDDING_API_KEY", "-u TRPC_AGENT_EMBEDDING_DIMENSIONS", "./cmd/trpc-embeddingcheck"} {
		if !strings.Contains(script, required) {
			t.Fatalf("script boundary missing: %s", required)
		}
	}
	if output, err := exec.Command("bash", "-n", "../check-embedding.sh").CombinedOutput(); err != nil {
		t.Fatalf("script syntax: %s", output)
	}
}
