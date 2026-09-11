package domain

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"testing"
)

func TestPostgresRedisSharedPhysicalTargetHasRoleBoundSnapshots(t *testing.T) {
	for _, kind := range []Kind{PostgreSQL, Redis} {
		t.Run(string(kind), func(t *testing.T) {
			entries := []Entry{{ID: "shared", Revision: 1, Label: "Shared backend", Kind: kind, Roles: []Role{Session, Memory}, Enabled: true, TenantIDs: []string{"tenant-a", "tenant-b"}}}
			directory, err := NewCatalog(entries)
			if err != nil {
				t.Fatal(err)
			}
			target := RuntimeTarget{BackendID: "shared", BackendRevision: 1, Kind: datav1.Kind(kind), Isolation: datav1.SessionIsolation, Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 2, MaxBytes: 1024}}
			if kind == PostgreSQL {
				target.Adapter = "managed-postgres-v1"
				target.PostgreSQL = &datav1.PostgresTarget{Host: "db.internal", Port: 5432, Database: "runtime", Username: "runtime", SSLMode: "verify-full"}
			} else {
				target.Adapter = "managed-redis-v1"
				target.Redis = &datav1.RedisTarget{Host: "redis.internal", Port: 6379, Username: "runtime", TLS: true}
			}
			c, err := NewRuntimeCatalog(directory, []RuntimeTarget{target})
			if err != nil {
				t.Fatal(err)
			}
			s, err := c.ResolveSnapshot("tenant-a", Selection{"shared", 1, Session})
			if err != nil {
				t.Fatal(err)
			}
			m, err := c.ResolveSnapshot("tenant-a", Selection{"shared", 1, Memory})
			if err != nil {
				t.Fatal(err)
			}
			if s.ValidateForRole("session") != nil || m.ValidateForRole("memory") != nil || s.Matches(m) {
				t.Fatal("role scope not fixed")
			}
			b, err := c.ResolveSnapshot("tenant-b", Selection{"shared", 1, Memory})
			if err != nil || m.Matches(b) {
				t.Fatal("tenant not bound")
			}
			if _, err = c.ResolveSnapshot("tenant-c", Selection{"shared", 1, Memory}); err != ErrNotAvailable {
				t.Fatal("unauthorized tenant", err)
			}
			// Entry eligibility, not physical implementation support, grants a role.
			entries[0].Roles = []Role{Session}
			directory, _ = NewCatalog(entries)
			c, err = NewRuntimeCatalog(directory, []RuntimeTarget{target})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.ResolveSnapshot("tenant-a", Selection{"shared", 1, Memory}); err != ErrCapability {
				t.Fatal("undeclared role granted", err)
			}
		})
	}
}
