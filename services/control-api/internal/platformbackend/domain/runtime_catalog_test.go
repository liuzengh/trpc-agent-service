package domain

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func TestRuntimeCatalogSharedTargetTenantBindingAndClone(t *testing.T) {
	es := []Entry{{ID: "pg", Revision: 1, Label: "Shared SQL", Kind: PostgreSQL, Roles: []Role{Session, Memory}, Enabled: true, TenantIDs: []string{"tenant-a", "tenant-b"}}}
	directory, _ := NewCatalog(es)
	target := RuntimeTarget{BackendID: "pg", BackendRevision: 1, Kind: datav1.PostgreSQL, Adapter: "managed-postgres-v1", Isolation: "tenant-session-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 8, MaxBytes: 1024}, PostgreSQL: &datav1.PostgresTarget{Host: "db.internal", Port: 5432, Database: "runtime", Username: "runtime_user", SSLMode: "verify-full"}}
	c, err := NewRuntimeCatalog(directory, []RuntimeTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	target.PostgreSQL.Host = "changed.internal"
	a, err := c.ResolveSnapshot("tenant-a", Selection{"pg", 1, Session})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.ResolveSnapshot("tenant-b", Selection{"pg", 1, Session})
	if err != nil {
		t.Fatal(err)
	}
	if a.PostgreSQL.Host != "db.internal" || *a.PostgreSQL != *b.PostgreSQL || a.Matches(b) {
		t.Fatal("shared target / tenant identity incorrect")
	}
	a.PostgreSQL.Host = "mutated.internal"
	again, _ := c.ResolveSnapshot("tenant-a", Selection{"pg", 1, Session})
	if again.PostgreSQL.Host != "db.internal" {
		t.Fatal("returned pointer aliases catalog")
	}
	if _, err := c.ResolveSnapshot("tenant-c", Selection{"pg", 1, Session}); err != ErrNotAvailable {
		t.Fatal(err)
	}
	if _, err := c.ResolveSnapshot("tenant-a", Selection{"pg", 2, Session}); err != ErrRevisionConflict {
		t.Fatal(err)
	}
	if _, err := c.ResolveSnapshot("tenant-a", Selection{"pg", 1, Artifact}); err != ErrCapability {
		t.Fatal(err)
	}
	if _, err := NewRuntimeCatalog(directory, nil); err != ErrInvalid {
		t.Fatal("enabled target omitted", err)
	}
	target.BackendRevision = 2
	if _, err := NewRuntimeCatalog(directory, []RuntimeTarget{target}); err != ErrInvalid {
		t.Fatal(err)
	}
}
