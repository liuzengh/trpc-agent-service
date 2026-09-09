package postgres

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"
	"time"
)

func TestLoadMigrationsSortsAndRequiresPairs(t *testing.T) {
	files := fstest.MapFS{
		"000002_second.up.sql":   {Data: []byte("select 2")},
		"000002_second.down.sql": {Data: []byte("select 2 down")},
		"000001_first.up.sql":    {Data: []byte("select 1")},
		"000001_first.down.sql":  {Data: []byte("select 1 down")},
	}
	migrations, err := loadMigrations(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 2 || migrations[0].version != 1 || migrations[1].version != 2 {
		t.Fatalf("unexpected migration order: %+v", migrations)
	}
	if migrations[0].checksum == "" || migrations[0].checksum == migrations[1].checksum {
		t.Fatal("expected stable, content-sensitive checksums")
	}

	_, err = loadMigrations(fstest.MapFS{"000001_only.up.sql": {Data: []byte("select 1")}})
	if !errors.Is(err, ErrInvalidMigrationSource) {
		t.Fatalf("expected invalid source error, got %v", err)
	}
}

func TestLoadMigrationsRejectsInvalidFiles(t *testing.T) {
	_, err := loadMigrations(fstest.MapFS{"bad.sql": {Data: []byte("not sql")}})
	if !errors.Is(err, ErrInvalidMigrationSource) {
		t.Fatalf("expected invalid source error, got %v", err)
	}
}

func TestPostgresConfigValidation(t *testing.T) {
	if _, err := (PostgresConfig{}).withDefaults(); err == nil {
		t.Fatal("expected empty URL to fail")
	}
	if _, err := (PostgresConfig{URL: "postgres://test", MinConns: 3, MaxConns: 2}).withDefaults(); err == nil {
		t.Fatal("expected invalid pool limits to fail")
	}
	cfg, err := (PostgresConfig{URL: "postgres://test"}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnectTimeout != 10*time.Second || cfg.LockTimeout != 30*time.Second || cfg.StatementTimeout != 10*time.Minute || cfg.MaxConns != 4 || cfg.MinConns != 1 || cfg.MigrationLockKey == 0 || cfg.AllowDestructiveDown {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestDestructiveDownRequiresExplicitOptIn(t *testing.T) {
	migrator := &migrator{}
	if err := migrator.Down(context.Background(), 1); !errors.Is(err, ErrDestructiveDownDisabled) {
		t.Fatalf("expected destructive down to be disabled, got %v", err)
	}
}

func TestRedactConnectionText(t *testing.T) {
	input := "dial postgres://user:secret@db.example.test:5432/app?password=secret failed"
	output := redactConnectionText(input)
	if output == input || containsAny(output, "secret", "postgres://") {
		t.Fatalf("connection details were not redacted: %q", output)
	}
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		for i := 0; i+len(candidate) <= len(value); i++ {
			if value[i:i+len(candidate)] == candidate {
				return true
			}
		}
	}
	return false
}
