//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/migrations"
)

// This test owns a fresh database: checksum tampering and schema rollback must
// never change the shared integration database or another test's migrations.
func TestMigrationChecksumLineEndingCompatibility(t *testing.T) {
	db := migrationChecksumDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runner := migrations.NewRunner(db)
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	scripts, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil || len(scripts) < 2 {
		t.Fatalf("read migration source list: count=%d err=%v", len(scripts), err)
	}
	lfScripts := make(map[string][]byte, len(scripts))
	versions := make([]string, 0, len(scripts))
	for _, path := range scripts {
		script, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		version := strings.TrimSuffix(filepath.Base(path), ".up.sql")
		lfScripts[version] = bytes.ReplaceAll(script, []byte("\r\n"), []byte("\n"))
		versions = append(versions, version)
		var stored string
		if err := db.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if want := checksumFixtureHash(lfScripts[version]); stored != want {
			t.Fatalf("new migration %s checksum=%s, want canonical=%s", version, stored, want)
		}
	}
	latest, previous := versions[len(versions)-1], versions[len(versions)-2]
	// Simulate a database populated by both original Windows and Linux release
	// binaries. Keep the latest migration LF and its predecessor CRLF.
	type stamp struct {
		checksum string
		applied  time.Time
	}
	history := make(map[string]stamp, len(versions))
	for n, version := range versions {
		script := lfScripts[version]
		if n%2 == 0 {
			script = bytes.ReplaceAll(script, []byte("\n"), []byte("\r\n"))
		}
		if version == latest {
			script = lfScripts[version]
		} else if version == previous {
			script = bytes.ReplaceAll(lfScripts[version], []byte("\n"), []byte("\r\n"))
		}
		entry := stamp{checksum: checksumFixtureHash(script)}
		if err := db.QueryRowContext(ctx, `UPDATE schema_migrations SET checksum=$2 WHERE version=$1 RETURNING applied_at`, version, entry.checksum).Scan(&entry.applied); err != nil {
			t.Fatal(err)
		}
		history[version] = entry
	}
	const sentinel = "checksum-compatibility-business-record"
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'preserved','active','{}')`, sentinel); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := runner.Up(ctx); err != nil {
			t.Fatalf("equivalent mixed release history rejected: %v", err)
		}
	}
	for version, before := range history {
		var after stamp
		if err := db.QueryRowContext(ctx, `SELECT checksum,applied_at FROM schema_migrations WHERE version=$1`, version).Scan(&after.checksum, &after.applied); err != nil {
			t.Fatal(err)
		}
		if before.checksum != after.checksum || !before.applied.Equal(after.applied) {
			t.Fatalf("accepted history was rewritten or reapplied: %s", version)
		}
	}
	var sentinelName string
	if err := db.QueryRowContext(ctx, `SELECT name FROM tenants WHERE id=$1`, sentinel).Scan(&sentinelName); err != nil || sentinelName != "preserved" {
		t.Fatalf("business data changed: name=%q err=%v", sentinelName, err)
	}
	t.Run("rollback accepts both historical endings", func(t *testing.T) {
		if err := runner.Down(ctx, 2); err != nil {
			t.Fatalf("LF and CRLF rollback: %v", err)
		}
		var remaining int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations WHERE version IN ($1,$2)`, latest, previous).Scan(&remaining); err != nil || remaining != 0 {
			t.Fatalf("rollback metadata count=%d err=%v", remaining, err)
		}
		if err := runner.Up(ctx); err != nil {
			t.Fatalf("reapply after mixed-ending rollback: %v", err)
		}
		for _, version := range []string{previous, latest} {
			var stored string
			if err := db.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&stored); err != nil || stored != checksumFixtureHash(lfScripts[version]) {
				t.Fatalf("reapplied migration is not canonical: %s err=%v", version, err)
			}
		}
	})
	for _, altered := range []struct {
		name   string
		script []byte
	}{
		{"changed SQL LF", append(append([]byte(nil), lfScripts[latest]...), []byte("SELECT 42;\n")...)},
		{"changed SQL CRLF", bytes.ReplaceAll(append(append([]byte(nil), lfScripts[latest]...), []byte("SELECT 42;\n")...), []byte("\n"), []byte("\r\n"))},
		{"mixed historical endings", bytes.Replace(lfScripts[latest], []byte("\n"), []byte("\r\n"), 1)},
	} {
		t.Run(altered.name, func(t *testing.T) {
			unknown := checksumFixtureHash(altered.script)
			if _, err := db.ExecContext(ctx, `UPDATE schema_migrations SET checksum=$2 WHERE version=$1`, latest, unknown); err != nil {
				t.Fatal(err)
			}
			for _, operation := range []func() error{func() error { return runner.Up(ctx) }, func() error { return runner.Down(ctx, 1) }} {
				if err := operation(); err == nil || !strings.Contains(err.Error(), "checksum drift") {
					t.Fatalf("unknown checksum must reject migration: %v", err)
				}
			}
			var stored string
			if err := db.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, latest).Scan(&stored); err != nil || stored != unknown {
				t.Fatalf("drift was silently replaced: checksum=%s err=%v", stored, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE schema_migrations SET checksum=$2 WHERE version=$1`, latest, checksumFixtureHash(lfScripts[latest])); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("untracked legacy checksum bootstraps once", func(t *testing.T) {
		if _, err := db.ExecContext(ctx, `UPDATE schema_migrations SET checksum='' WHERE version=$1`, latest); err != nil {
			t.Fatal(err)
		}
		if err := runner.Down(ctx, 1); err == nil || !strings.Contains(err.Error(), "checksum drift") {
			t.Fatalf("empty checksum authorized rollback: %v", err)
		}
		if err := runner.Up(ctx); err != nil {
			t.Fatal(err)
		}
		var stored string
		if err := db.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, latest).Scan(&stored); err != nil || stored != checksumFixtureHash(lfScripts[latest]) {
			t.Fatalf("bootstrap checksum=%s err=%v", stored, err)
		}
	})
}

func checksumFixtureHash(script []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(script))
}

func migrationChecksumDatabase(t *testing.T) *sql.DB {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		t.Fatal("migration checksum acceptance requires TEST_DATABASE_URL")
	}
	admin, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal("open migration database creation connection")
	}
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := "migration_checksum_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+pq.QuoteIdentifier(name)); err != nil {
		t.Fatal("create owned migration test database (TEST_DATABASE_URL role requires CREATEDB)")
	}
	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, `DROP DATABASE `+pq.QuoteIdentifier(name)); err != nil {
			t.Errorf("drop owned migration test database: %v", err)
		}
	})
	parsed.Path, parsed.RawPath = "/"+name, ""
	db, err = sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal("open owned migration test database")
	}
	return db
}
