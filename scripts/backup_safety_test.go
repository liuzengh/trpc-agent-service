package scripts

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestBackupDrillIsIsolatedAndSyntaxValid(t *testing.T) {
	path := "e2e-backup-restore.sh"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	script := string(data)
	for _, forbidden := range []string{"docker compose", "dropdb", "redis-cli DEL", "--rdb", "rm -rf", "trpc_agent"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("forbidden shared-data operation: %s", forbidden)
		}
	}
	for _, required := range []string{"--network none", "--cidfile", `[[ "$label" != "$DRILL_ID" ]]`, `--tmpfs /var/lib/postgresql/data:rw`, `pg_restore --exit-on-error`, `target=/data/dump.rdb,readonly`, `trap cleanup EXIT`} {
		if !strings.Contains(script, required) {
			t.Fatalf("missing isolation guard %s", required)
		}
	}
	if out, err := exec.Command("bash", "-n", path).CombinedOutput(); err != nil {
		t.Fatalf("bash syntax: %s", out)
	}
}
