package dbbinding

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func parsePoolConfig(t *testing.T, dsn string) *pgxpool.Config {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	return cfg
}

func TestIdentityExcludesCredentialsAndRole(t *testing.T) {
	first := parsePoolConfig(t, "postgres://queue:secret-a@db.internal:5432/agents?search_path=runtime")
	second := parsePoolConfig(t, "postgres://session:secret-b@db.internal:5432/agents?search_path=runtime")
	if got, want := FromConnConfig(first.ConnConfig), FromConnConfig(second.ConnConfig); got != want {
		t.Fatalf("same database binding produced different identities: %q != %q", got, want)
	}
	if got := FromConnConfig(first.ConnConfig); got == "" || got == FromConnConfig(nil) {
		t.Fatalf("valid config produced invalid identity %q", got)
	}
}

func TestIdentitySeparatesDatabaseHostAndSearchPath(t *testing.T) {
	base := FromConnConfig(parsePoolConfig(t, "postgres://role@db-a:5432/agents?search_path=runtime").ConnConfig)
	for name, dsn := range map[string]string{
		"database":    "postgres://role@db-a:5432/other?search_path=runtime",
		"host":        "postgres://role@db-b:5432/agents?search_path=runtime",
		"port":        "postgres://role@db-a:6432/agents?search_path=runtime",
		"search_path": "postgres://role@db-a:5432/agents?search_path=other",
	} {
		t.Run(name, func(t *testing.T) {
			if got := FromConnConfig(parsePoolConfig(t, dsn).ConnConfig); got == base {
				t.Fatalf("%s change did not change database identity", name)
			}
		})
	}
}
