package scripts

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestRegressionDoesNotInheritLiveIntegrationsOrRestartServices(t *testing.T) {
	data, err := os.ReadFile("regression.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(data)
	for _, forbidden := range []string{"source .env", "docker compose", "start-real.sh", "stop.sh", "check-model.sh", "rm -rf"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("regression must not operate live services: %s", forbidden)
		}
	}
	for _, required := range []string{"unset TEST_POSTGRES_URL TEST_REDIS_URL TEST_S3_ENDPOINT TEST_QDRANT_HOST TEST_QDRANT_PORT", "unset TEST_TRACE_OTLP_ENDPOINT TEST_PERMISSIONS_DOCKER TEST_RECOVERY_DOCKER", "--pull=never --network none", "go test -race ./..."} {
		if !strings.Contains(script, required) {
			t.Fatalf("missing regression safety boundary: %s", required)
		}
	}
	if out, err := exec.Command("bash", "-n", "regression.sh").CombinedOutput(); err != nil {
		t.Fatalf("syntax: %s", out)
	}
}
