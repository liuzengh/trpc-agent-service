package configfile

import (
	"crypto/sha256"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validRuntime = `{"version":"v1","backends":[{"backend_id":"redis","backend_revision":2,"kind":"redis","adapter":"managed-redis-v1","isolation":"tenant-session-v1","limits":{"timeout_ms":1000,"max_concurrency":2,"max_bytes":1024},"redis":{"host":"redis.private","port":6379,"database":0,"username":"runtime","tls":true}}]}`

func TestLoadPrivateRuntimeCatalogPinnedAndScoped(t *testing.T) {
	directory, err := Decode([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "targets.json")
	if err := os.WriteFile(path, []byte(validRuntime), 0600); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(validRuntime)))
	c, err := LoadRuntime(path, digest, directory)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.ResolveSnapshot("tenant-a", domain.Selection{BackendID: "redis", Revision: 2, Role: domain.Session})
	if err != nil || snapshot.Redis.Host != "redis.private" {
		t.Fatal(snapshot, err)
	}
	if _, err := c.ResolveSnapshot("tenant-b", domain.Selection{BackendID: "redis", Revision: 2, Role: domain.Session}); err != domain.ErrNotAvailable {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntime(path, digest, directory); err != ErrConfig {
		t.Fatal("accepted world-readable private targets", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntime(path, strings.Repeat("0", 64), directory); err != ErrConfig {
		t.Fatal(err)
	}
	if got, err := LoadRuntime("", "", directory); err != nil || got != nil {
		t.Fatal(got, err)
	}
}
func TestRuntimeConfigRejectsMalformedOrMismatchedDefinitions(t *testing.T) {
	c, _ := Decode([]byte(valid))
	for _, bad := range []string{
		strings.Replace(validRuntime, `"backend_id":"redis"`, `"backend_id":"redis","backend_id":"other"`, 1),
		strings.Replace(validRuntime, `"host":"redis.private"`, `"Host":"redis.private"`, 1),
		strings.Replace(validRuntime, `"tls":true`, `"tls":null`, 1),
		strings.Replace(validRuntime, `,"tls":true`, "", 1),
		strings.Replace(validRuntime, `"username":"runtime"`, `"username":"runtime","password":"private"`, 1),
		strings.Replace(validRuntime, `"backend_revision":2`, `"backend_revision":3`, 1),
		strings.Replace(validRuntime, `"backend_id":"redis"`, `"backend_id":"other"`, 1),
		`{"version":"v1","backends":[]}`, validRuntime + `{}`, `{"version":"v1","backends":null}`,
	} {
		if _, err := DecodeRuntime([]byte(bad), c); err != ErrConfig {
			t.Fatalf("accepted invalid private target: %s; %v", bad, err)
		}
	}
}

// Exercise the actual deployment examples, not a separately maintained fixture.
func TestDeploymentExamplesResolveAllFourBackendKinds(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..", "..", "..", "..")
	dirBytes, err := os.ReadFile(filepath.Join(root, "deploy/compose/managed-data/catalog.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	targetBytes, err := os.ReadFile(filepath.Join(root, "deploy/compose/managed-data/targets.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	directory, err := Decode(dirBytes)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "targets.json")
	if err := os.WriteFile(path, targetBytes, 0600); err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadRuntime(path, fmt.Sprintf("%x", sha256.Sum256(targetBytes)), directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id   string
		kind domain.Kind
		role domain.Role
	}{
		{"sessions-pg", domain.PostgreSQL, domain.Session},
		{"sessions-pg", domain.PostgreSQL, domain.Memory},
		{"sessions-redis", domain.Redis, domain.Session},
		{"sessions-redis", domain.Redis, domain.Memory},
		{"knowledge", domain.Qdrant, domain.Knowledge},
		{"artifacts", domain.S3, domain.Artifact},
	} {
		t.Run(tc.id+"/"+string(tc.role), func(t *testing.T) {
			selection := domain.Selection{BackendID: tc.id, Revision: 1, Role: tc.role}
			snapshot, err := catalog.ResolveSnapshot("tenant-example", selection)
			if err != nil {
				t.Fatal(err)
			}
			if string(snapshot.Kind) != string(tc.kind) || snapshot.TenantID != "tenant-example" {
				t.Fatal("wrong snapshot identity")
			}
			if _, err := catalog.ResolveSnapshot("tenant-other", selection); err != domain.ErrNotAvailable {
				t.Fatal("cross-tenant resolution", err)
			}
			selection.Revision = 2
			if _, err := catalog.ResolveSnapshot("tenant-example", selection); err != domain.ErrRevisionConflict {
				t.Fatal("revision fallback", err)
			}
		})
	}
}
