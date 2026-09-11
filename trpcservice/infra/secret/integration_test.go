//go:build integration

package secret

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

func newMySQLStoreForTest(t *testing.T) *MySQLStore {
	t.Helper()
	ctx := context.Background()

	// This package sits three levels below the repo root
	// (trpcservice/infra/secret), so the schema lives at ../../../deployments.
	// The old two-level path silently globbed nothing, and the test then failed
	// with "Table 'test.secrets' doesn't exist" — a broken test, not a broken
	// store.
	scripts, err := filepath.Glob(filepath.Join("..", "..", "..", "deployments", "mysql", "init", "*.sql"))
	if err != nil {
		t.Fatalf("glob init sql: %v", err)
	}
	if len(scripts) == 0 {
		t.Fatal("no init sql found: the schema this test needs was never loaded")
	}
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"),
		mysql.WithScripts(scripts...))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s, err := NewMySQLStore(db, "test-master-key")
	if err != nil {
		t.Fatalf("NewMySQLStore: %v", err)
	}
	return s
}

func TestMySQLSecretRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newMySQLStoreForTest(t)

	if err := s.Put(ctx, "endpoint:gpt-4", "sk-abc123"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "endpoint:gpt-4")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "sk-abc123" {
		t.Errorf("Get = %q, want sk-abc123", got)
	}

	// value is not stored in plaintext anywhere in the row
	rows, err := s.db.Query(`SELECT ciphertext FROM secrets WHERE secret_key = 'endpoint:gpt-4'`)
	if err != nil {
		t.Fatalf("query raw: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a ciphertext row")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		t.Fatalf("scan raw: %v", err)
	}
	if string(raw) == "sk-abc123" || strings.Contains(string(raw), "sk-abc123") {
		t.Error("ciphertext column must not contain the plaintext")
	}

	// missing key -> ErrNotFound
	if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get missing = %v, want ErrNotFound", err)
	}

	// list metadata only
	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Key != "endpoint:gpt-4" {
		t.Errorf("List = %+v, want [endpoint:gpt-4]", list)
	}

	// delete
	if err := s.Delete(ctx, "endpoint:gpt-4"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, "endpoint:gpt-4"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
}

func TestMySQLSecretDifferentMasterKeyCannotDecrypt(t *testing.T) {
	ctx := context.Background()
	s := newMySQLStoreForTest(t)

	if err := s.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// a store with a different master key reads the same row but cannot decrypt
	other, err := NewMySQLStore(s.db, "different-master-key")
	if err != nil {
		t.Fatalf("NewMySQLStore(other): %v", err)
	}
	if _, err := other.Get(ctx, "k"); err == nil {
		t.Error("different master key must fail to decrypt")
	}
}
