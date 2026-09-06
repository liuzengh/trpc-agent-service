package scripts

import (
	"os"
	"strings"
	"testing"
)

func TestDockerContextExcludesPrivateConfigAndBackups(t *testing.T) {
	data, err := os.ReadFile("../.dockerignore")
	if err != nil {
		t.Fatal(err)
	}
	rules := "\n" + string(data) + "\n"
	for _, required := range []string{".env*", "**/.env*", "**/*.env", "data", ".git", "bin"} {
		if !strings.Contains(rules, "\n"+required+"\n") {
			t.Fatalf("private path exclusion missing: %s", required)
		}
	}
}
